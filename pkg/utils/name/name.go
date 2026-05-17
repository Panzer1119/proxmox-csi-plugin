package name

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/google/uuid"
	"k8s.io/klog/v2"
)

var invalidDatasetRegExp = regexp.MustCompile(`[^\w-.]+`)

// UUIDInput returns a length-prefixed identity string for deterministic UUID generation.
func UUIDInput(namespace, name string) string {
	return fmt.Sprintf("%d:%s|%d:%s", len(namespace), namespace, len(name), name)
}

// UUID returns a UUIDv5 derived from the supplied uuid namespace and name.
func UUID(uuidNamespace, name string) (uuid.UUID, error) {
	var ns uuid.UUID
	var err error
	if uuidNamespace != "" {
		ns, err = uuid.Parse(uuidNamespace)
		if err != nil {
			return uuid.Nil, fmt.Errorf("invalid uuid namespace: %w", err)
		}
	} else {
		ns = uuid.NameSpaceOID
	}

	return uuid.NewSHA1(ns, []byte(name)), nil
}

// DeriveUUID returns a UUIDv5 derived from the supplied uuid namespace, namespace, and name.
func DeriveUUID(uuidNamespace, namespace, name string) (uuid.UUID, error) {
	if strings.TrimSpace(uuidNamespace) == "" {
		return uuid.Nil, nil
	}

	identity := UUIDInput(namespace, name)
	u, err := UUID(uuidNamespace, identity)
	if err != nil {
		return uuid.Nil, err
	}

	return u, nil
}

// RenderTemplate renders a Go text/template with missing keys treated as errors.
func RenderTemplate(templateText string, ctx any) (string, error) {
	tpl, err := template.New("name").Option("missingkey=error").Parse(templateText)
	if err != nil {
		return "", err
	}

	var b bytes.Buffer
	if err := tpl.Execute(&b, ctx); err != nil {
		return "", err
	}

	return strings.TrimSpace(b.String()), nil
}

func NormalizeName(name string, maxLength int) (string, error) {
	sanitized := Sanitize(name)
	klog.V(5).InfoS(
		"NormalizeName: sanitized name",
		"original", name,
		"sanitized", sanitized,
	)

	if sanitized == "" {
		return "", fmt.Errorf("name is empty after sanitization")
	}

	if len(sanitized) > maxLength {
		truncated := strings.TrimRight(sanitized[:maxLength], "-")
		klog.V(5).InfoS(
			"NormalizeName: truncated name",
			"originalLength", len(sanitized),
			"finalLength", len(truncated),
		)
		return truncated, nil
	}

	return sanitized, nil
}

func Sanitize(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.NewReplacer("_", "-", ".", "-").Replace(s)
	s = invalidDatasetRegExp.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")

	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}

	return s
}
