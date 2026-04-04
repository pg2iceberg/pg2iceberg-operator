/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	replicationv1alpha1 "github.com/pg2iceberg/pg2iceberg-operator/api/v1alpha1"
)

const (
	requeueHealth = 30 * time.Second
	requeueError  = 10 * time.Second

	conditionTypeReady       = "Ready"
	conditionTypeProgressing = "Progressing"
	conditionTypeDegraded    = "Degraded"
)

// Pg2IcebergReconciler reconciles a Pg2Iceberg object.
type Pg2IcebergReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=replication.pg2iceberg.io,resources=pg2icebergs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=replication.pg2iceberg.io,resources=pg2icebergs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=replication.pg2iceberg.io,resources=pg2icebergs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

func (r *Pg2IcebergReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1. Fetch the CR.
	var cr replicationv1alpha1.Pg2Iceberg
	if err := r.Get(ctx, req.NamespacedName, &cr); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Resolve secrets.
	secrets, err := r.resolveSecrets(ctx, &cr)
	if err != nil {
		log.Error(err, "failed to resolve secrets")
		meta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:               conditionTypeDegraded,
			Status:             metav1.ConditionTrue,
			Reason:             "SecretResolutionFailed",
			Message:            err.Error(),
			ObservedGeneration: cr.Generation,
		})
		cr.Status.Phase = "Pending"
		_ = r.Status().Update(ctx, &cr)
		return ctrl.Result{RequeueAfter: requeueError}, nil
	}

	// 3. Generate config.yaml.
	cfgYAML, err := buildConfigYAML(&cr.Spec, secrets)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("build config: %w", err)
	}
	hash := configHash(cfgYAML)

	// 4. Create/Update ConfigMap.
	cm := buildConfigMap(&cr, cfgYAML)
	if err := controllerutil.SetControllerReference(&cr, cm, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.createOrUpdate(ctx, cm); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configmap: %w", err)
	}

	// 5. Determine desired replicas (suspend, scale-to-zero).
	desiredReplicas := deploymentReplicaCount(&cr)

	// 6. Create/Update Deployment.
	dep := buildDeployment(&cr, hash)
	dep.Spec.Replicas = &desiredReplicas
	if err := controllerutil.SetControllerReference(&cr, dep, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.createOrUpdate(ctx, dep); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile deployment: %w", err)
	}

	// 7. Create/Update metrics Service.
	svc := buildMetricsService(&cr)
	if err := controllerutil.SetControllerReference(&cr, svc, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.createOrUpdate(ctx, svc); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile service: %w", err)
	}

	// 8. Update status.
	cr.Status.ObservedGeneration = cr.Generation
	cr.Status.ConfigHash = hash

	// Handle suspended state.
	if cr.Spec.Suspend != nil && *cr.Spec.Suspend {
		cr.Status.Phase = "Suspended"
		cr.Status.PipelineStatus = ""
		meta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             "Suspended",
			Message:            "Pipeline is suspended",
			ObservedGeneration: cr.Generation,
		})
		meta.RemoveStatusCondition(&cr.Status.Conditions, conditionTypeDegraded)
		return ctrl.Result{}, r.Status().Update(ctx, &cr)
	}

	// Handle scaled-to-zero state: check for wake.
	if cr.Status.ScaledToZero {
		return r.reconcileScaledToZero(ctx, &cr, secrets)
	}

	// 9. Poll health if running.
	if desiredReplicas > 0 {
		metricsPort := int32(9090)
		if cr.Spec.MetricsPort != nil {
			metricsPort = *cr.Spec.MetricsPort
		}
		health, err := pollHealth(ctx, cr.Name+"-metrics", cr.Namespace, metricsPort)
		if err != nil {
			log.V(1).Info("health check failed", "error", err)
			cr.Status.Phase = "Pending"
			meta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
				Type:               conditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             "HealthCheckFailed",
				Message:            err.Error(),
				ObservedGeneration: cr.Generation,
			})
		} else {
			r.applyHealthStatus(&cr, health)
			// 10. Check for idle (scale-to-zero).
			r.checkIdle(ctx, &cr, health)
		}
	}

	if err := r.Status().Update(ctx, &cr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueHealth}, nil
}

// resolveSecrets reads secret values referenced by the CR spec.
func (r *Pg2IcebergReconciler) resolveSecrets(ctx context.Context, cr *replicationv1alpha1.Pg2Iceberg) (resolvedSecrets, error) {
	var s resolvedSecrets

	pgPass, err := r.readSecretKey(ctx, cr.Namespace, cr.Spec.Source.Postgres.PasswordRef)
	if err != nil {
		return s, fmt.Errorf("postgres password: %w", err)
	}
	s.pgPassword = pgPass

	s3Access, err := r.readSecretKey(ctx, cr.Namespace, cr.Spec.Sink.S3.AccessKeyRef)
	if err != nil {
		return s, fmt.Errorf("s3 access key: %w", err)
	}
	s.s3AccessKey = s3Access

	s3Secret, err := r.readSecretKey(ctx, cr.Namespace, cr.Spec.Sink.S3.SecretKeyRef)
	if err != nil {
		return s, fmt.Errorf("s3 secret key: %w", err)
	}
	s.s3SecretKey = s3Secret

	if cr.Spec.State != nil && cr.Spec.State.PostgresURLRef != nil {
		stateURL, err := r.readSecretKey(ctx, cr.Namespace, *cr.Spec.State.PostgresURLRef)
		if err != nil {
			return s, fmt.Errorf("state postgres url: %w", err)
		}
		s.statePostgre = stateURL
	}

	return s, nil
}

// readSecretKey reads a single key from a Secret.
func (r *Pg2IcebergReconciler) readSecretKey(ctx context.Context, namespace string, ref corev1.SecretKeySelector) (string, error) {
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: namespace}, &secret); err != nil {
		return "", fmt.Errorf("get secret %s: %w", ref.Name, err)
	}
	val, ok := secret.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %s", ref.Key, ref.Name)
	}
	return string(val), nil
}

// applyHealthStatus maps a /healthz response to CR status fields.
func (r *Pg2IcebergReconciler) applyHealthStatus(cr *replicationv1alpha1.Pg2Iceberg, health *healthResponse) {
	cr.Status.PipelineStatus = health.Status
	cr.Status.RowsProcessed = health.RowsProcessed
	cr.Status.ReplicationLSN = health.LSN
	cr.Status.LastFlushAt = health.LastFlushAt
	cr.Status.Uptime = health.Uptime

	switch health.Status {
	case "running":
		cr.Status.Phase = "Running"
		meta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionTrue,
			Reason:             "PipelineRunning",
			Message:            "Pipeline is actively replicating",
			ObservedGeneration: cr.Generation,
		})
		meta.RemoveStatusCondition(&cr.Status.Conditions, conditionTypeDegraded)

	case "snapshotting":
		cr.Status.Phase = "Snapshotting"
		meta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             metav1.ConditionTrue,
			Reason:             "Snapshotting",
			Message:            "Initial table snapshot in progress",
			ObservedGeneration: cr.Generation,
		})

	case "starting":
		cr.Status.Phase = "Pending"
		meta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             metav1.ConditionTrue,
			Reason:             "Starting",
			Message:            "Pipeline is starting up",
			ObservedGeneration: cr.Generation,
		})

	case "error":
		cr.Status.Phase = "Error"
		meta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:               conditionTypeDegraded,
			Status:             metav1.ConditionTrue,
			Reason:             "PipelineError",
			Message:            "Pipeline reported error state",
			ObservedGeneration: cr.Generation,
		})
		meta.SetStatusCondition(&cr.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             "PipelineError",
			Message:            "Pipeline reported error state",
			ObservedGeneration: cr.Generation,
		})
	}
}

// checkIdle evaluates whether the pipeline is idle and should be scaled to zero.
func (r *Pg2IcebergReconciler) checkIdle(ctx context.Context, cr *replicationv1alpha1.Pg2Iceberg, health *healthResponse) {
	if cr.Spec.ScaleToZero == nil || !cr.Spec.ScaleToZero.Enabled {
		return
	}
	if health.Status != "running" {
		// Only consider idle when pipeline is fully running.
		cr.Status.IdleSince = ""
		return
	}

	// Compare current rows to previously observed.
	prevRows := cr.Status.RowsProcessed
	if health.RowsProcessed != prevRows && prevRows != 0 {
		// Pipeline is active — clear idle timer.
		cr.Status.IdleSince = ""
		return
	}

	// Pipeline is idle (no new rows).
	now := time.Now()
	if cr.Status.IdleSince == "" {
		cr.Status.IdleSince = now.Format(time.RFC3339)
		return
	}

	idleSince, err := time.Parse(time.RFC3339, cr.Status.IdleSince)
	if err != nil {
		cr.Status.IdleSince = now.Format(time.RFC3339)
		return
	}

	timeout := 15 * time.Minute
	if cr.Spec.ScaleToZero.IdleTimeout != "" {
		if d, err := time.ParseDuration(cr.Spec.ScaleToZero.IdleTimeout); err == nil {
			timeout = d
		}
	}

	if now.Sub(idleSince) >= timeout {
		log := logf.FromContext(ctx)
		log.Info("pipeline idle, scaling to zero", "idleSince", cr.Status.IdleSince)
		cr.Status.ScaledToZero = true
		cr.Status.Phase = "Idle"
	}
}

// reconcileScaledToZero checks if the pipeline should wake up from idle scale-down.
func (r *Pg2IcebergReconciler) reconcileScaledToZero(ctx context.Context, cr *replicationv1alpha1.Pg2Iceberg, secrets resolvedSecrets) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	wakeInterval := 30 * time.Second
	if cr.Spec.ScaleToZero != nil && cr.Spec.ScaleToZero.WakeCheckInterval != "" {
		if d, err := time.ParseDuration(cr.Spec.ScaleToZero.WakeCheckInterval); err == nil {
			wakeInterval = d
		}
	}

	wake, err := checkWake(ctx, &cr.Spec, secrets)
	cr.Status.LastWakeCheck = time.Now().Format(time.RFC3339)

	if err != nil {
		log.Error(err, "wake check failed")
		if err := r.Status().Update(ctx, cr); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: wakeInterval}, nil
	}

	if wake {
		log.Info("changes detected, waking pipeline")
		cr.Status.ScaledToZero = false
		cr.Status.IdleSince = ""
		cr.Status.Phase = "Pending"

		// Scale the Deployment back to 1.
		var dep appsv1.Deployment
		if err := r.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, &dep); err == nil {
			one := int32(1)
			dep.Spec.Replicas = &one
			if err := r.Update(ctx, &dep); err != nil {
				return ctrl.Result{}, fmt.Errorf("scale up deployment: %w", err)
			}
		}
	}

	if err := r.Status().Update(ctx, cr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: wakeInterval}, nil
}

// createOrUpdate creates or updates a Kubernetes object.
func (r *Pg2IcebergReconciler) createOrUpdate(ctx context.Context, obj client.Object) error {
	existing := obj.DeepCopyObject().(client.Object)
	err := r.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, obj)
	}
	if err != nil {
		return err
	}

	// Preserve resource version for update.
	obj.SetResourceVersion(existing.GetResourceVersion())

	// Skip update if spec hasn't changed (reduce API calls).
	if equality.Semantic.DeepEqual(existing, obj) {
		return nil
	}

	return r.Update(ctx, obj)
}

// SetupWithManager sets up the controller with the Manager.
func (r *Pg2IcebergReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&replicationv1alpha1.Pg2Iceberg{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Named("pg2iceberg").
		Complete(r)
}
