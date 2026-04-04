package controller

import (
	"math"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	replicationv1alpha1 "github.com/pg2iceberg/pg2iceberg-operator/api/v1alpha1"
)

const (
	configVolumeName = "config"
	configMountPath  = "/etc/pg2iceberg"
	configFileName   = "config.yaml"
	configHashAnnotation = "pg2iceberg.io/config-hash"
)

// buildDeployment creates the Deployment spec for the pg2iceberg container.
func buildDeployment(cr *replicationv1alpha1.Pg2Iceberg, hash string) *appsv1.Deployment {
	replicas := int32(1)
	if cr.Spec.Suspend != nil && *cr.Spec.Suspend {
		replicas = 0
	}

	image := cr.Spec.Image
	if image == "" {
		image = "ghcr.io/pg2iceberg/pg2iceberg:latest"
	}

	metricsPort := int32(9090)
	if cr.Spec.MetricsPort != nil {
		metricsPort = *cr.Spec.MetricsPort
	}

	// Build environment variables.
	env := []corev1.EnvVar{
		{Name: "GOGC", Value: "off"},
		{Name: "GOMEMLIMIT", Value: computeGoMemLimit(cr)},
	}
	env = append(env, cr.Spec.ExtraEnv...)

	// Pod labels and annotations.
	podLabels := commonLabels(cr)
	podAnnotations := map[string]string{
		configHashAnnotation: hash,
	}
	if cr.Spec.PodTemplate != nil {
		for k, v := range cr.Spec.PodTemplate.Labels {
			podLabels[k] = v
		}
		for k, v := range cr.Spec.PodTemplate.Annotations {
			podAnnotations[k] = v
		}
	}

	container := corev1.Container{
		Name:  "pg2iceberg",
		Image: image,
		Args:  []string{"--config", configMountPath + "/" + configFileName},
		Ports: []corev1.ContainerPort{
			{
				Name:          "metrics",
				ContainerPort: metricsPort,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Env:       env,
		Resources: cr.Spec.Resources,
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz",
					Port: intstr.FromInt32(metricsPort),
				},
			},
			InitialDelaySeconds: 15,
			PeriodSeconds:       10,
			TimeoutSeconds:      5,
			FailureThreshold:    3,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz",
					Port: intstr.FromInt32(metricsPort),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			TimeoutSeconds:      3,
			FailureThreshold:    3,
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      configVolumeName,
				MountPath: configMountPath,
				ReadOnly:  true,
			},
		},
	}

	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{container},
		Volumes: []corev1.Volume{
			{
				Name: configVolumeName,
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: cr.Name + "-config",
						},
					},
				},
			},
		},
		TerminationGracePeriodSeconds: ptr.To[int64](90),
	}

	// Apply pod template overrides.
	if t := cr.Spec.PodTemplate; t != nil {
		if t.NodeSelector != nil {
			podSpec.NodeSelector = t.NodeSelector
		}
		if t.Tolerations != nil {
			podSpec.Tolerations = t.Tolerations
		}
		if t.Affinity != nil {
			podSpec.Affinity = t.Affinity
		}
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    commonLabels(cr),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: commonLabels(cr),
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},
				Spec: podSpec,
			},
		},
	}

	return dep
}

// computeGoMemLimit returns the GOMEMLIMIT value.
// Uses the explicit spec value if set, otherwise 80% of memory limit.
func computeGoMemLimit(cr *replicationv1alpha1.Pg2Iceberg) string {
	if cr.Spec.GoMemLimit != "" {
		return cr.Spec.GoMemLimit
	}

	memLimit := cr.Spec.Resources.Limits[corev1.ResourceMemory]
	if memLimit.IsZero() {
		return "1600MiB"
	}

	bytes := memLimit.Value()
	mib := int64(math.Floor(float64(bytes) * 0.8 / (1024 * 1024)))
	return strconv.FormatInt(mib, 10) + "MiB"
}

// deploymentReplicaCount returns the desired replica count based on CR state.
func deploymentReplicaCount(cr *replicationv1alpha1.Pg2Iceberg) int32 {
	if cr.Spec.Suspend != nil && *cr.Spec.Suspend {
		return 0
	}
	if cr.Status.ScaledToZero {
		return 0
	}
	return 1
}

