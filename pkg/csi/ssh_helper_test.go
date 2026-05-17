package csi

import (
	"context"
	"testing"

	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSSHHelperBuildSSHClientConfigFallsBackToClusterDefaults(t *testing.T) {
	t.Parallel()

	kclient := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-auth", Namespace: "default"},
			Data: map[string][]byte{
				"password": []byte("cluster-password"),
				"id_rsa":   []byte("cluster-private-key"),
			},
		},
	)

	host, cfg, err := buildSSHClientConfig(context.Background(), &ControllerService{kclient: kclient}, &pxpool.ProxmoxCluster{
		SSHUser:                "root",
		SSHPasswordSecretRef:   &pxpool.SecretKeyRef{Name: "cluster-auth", Key: "password"},
		SSHPrivateKeySecretRef: &pxpool.SecretKeyRef{Name: "cluster-auth", Key: "id_rsa"},
		SSHPort:                22,
		SSHUseSudo:             false,
		NodeSSHOptions: map[string]*pxpool.SSHOptions{
			"node-1": {
				Host: "10.0.0.11",
			},
		},
	}, "node-1")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.11", host)
	assert.Equal(t, "root", cfg.User)
	assert.Equal(t, 22, cfg.Port)
	assert.False(t, cfg.UseSudo)
	assert.Equal(t, "cluster-password", cfg.Password)
	assert.Equal(t, "cluster-private-key", cfg.PrivateKey)
}

func TestSSHHelperBuildSSHClientConfigAppliesNodeOverrides(t *testing.T) {
	t.Parallel()

	trueValue := true
	kclient := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-auth", Namespace: "default"},
			Data: map[string][]byte{
				"password": []byte("cluster-password"),
				"id_rsa":   []byte("cluster-private-key"),
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "node-auth", Namespace: "default"},
			Data: map[string][]byte{
				"password": []byte("node-password"),
			},
		},
	)

	host, cfg, err := buildSSHClientConfig(context.Background(), &ControllerService{kclient: kclient}, &pxpool.ProxmoxCluster{
		SSHUser:                "root",
		SSHPasswordSecretRef:   &pxpool.SecretKeyRef{Name: "cluster-auth", Key: "password"},
		SSHPrivateKeySecretRef: &pxpool.SecretKeyRef{Name: "cluster-auth", Key: "id_rsa"},
		SSHPort:                22,
		SSHUseSudo:             false,
		NodeSSHOptions: map[string]*pxpool.SSHOptions{
			"node-1": {
				Host:                 "10.0.0.21",
				SSHUser:              "ubuntu",
				SSHPasswordSecretRef: &pxpool.SecretKeyRef{Name: "node-auth", Key: "password"},
				SSHPort:              2222,
				SSHUseSudo:           &trueValue,
			},
		},
	}, "node-1")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.21", host)
	assert.Equal(t, "ubuntu", cfg.User)
	assert.Equal(t, 2222, cfg.Port)
	assert.True(t, cfg.UseSudo)
	assert.Equal(t, "node-password", cfg.Password)
	assert.Equal(t, "cluster-private-key", cfg.PrivateKey)
}
