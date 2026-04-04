package controller

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	replicationv1alpha1 "github.com/pg2iceberg/pg2iceberg-operator/api/v1alpha1"
)

// buildMetricsService creates the ClusterIP Service for Prometheus scraping.
func buildMetricsService(cr *replicationv1alpha1.Pg2Iceberg) *corev1.Service {
	metricsPort := int32(9090)
	if cr.Spec.MetricsPort != nil {
		metricsPort = *cr.Spec.MetricsPort
	}

	labels := commonLabels(cr)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-metrics",
			Namespace: cr.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   strconv.Itoa(int(metricsPort)),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: commonLabels(cr),
			Ports: []corev1.ServicePort{
				{
					Name:     "metrics",
					Port:     metricsPort,
					Protocol: corev1.ProtocolTCP,
				},
			},
		},
	}
}
