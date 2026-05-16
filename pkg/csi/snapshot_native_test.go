package csi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNativeSnapshotIDRoundTripJSON(t *testing.T) {
	id := nativeSnapshotID{
		Region:   "cluster-1",
		Zone:     "node-1",
		Storage:  "zfspool",
		Dataset:  "ultra-fast/proxmox/storage/vm-100-disk-0",
		Snapshot: "vs-20260517T120000Z-123e4567-e89b-12d3-a456-426614174000",
		Metadata: map[string]string{
			"policy": "release-destroy",
			"hold":   "pvecsi-123e4567-e89b-12d3-a456-426614174000",
		},
	}

	encoded, err := id.String()
	require.NoError(t, err)

	decoded, err := parseNativeSnapshotID(encoded)
	require.NoError(t, err)
	assert.Equal(t, &id, decoded)
}

func TestGenerateSnapshotNameUsesLengthPrefixedInput(t *testing.T) {
	tmpl := "{{ .UUID }}"

	left, err := generateSnapshotName(tmpl, "", "", "ab", "c")
	require.NoError(t, err)
	right, err := generateSnapshotName(tmpl, "", "", "a", "bc")
	require.NoError(t, err)

	assert.NotEqual(t, left, right)
	assert.Len(t, left, 36)
	assert.Len(t, right, 36)
}

func TestComputeHoldTagUsesFullUUID(t *testing.T) {
	// pass empty uuidNamespace to preserve legacy behavior (uses NameSpaceOID)
	tag := computeHoldTag("", "namespace", "snapshot-name")
	assert.True(t, len(tag) > len("pvecsi-"))
	assert.Equal(t, 43, len(tag))
	assert.Contains(t, tag, "-")
}
