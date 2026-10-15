package creds

import (
	"crypto/x509"
	"encoding/pem"
	"testing"
)

func pemEncodeCert(t *testing.T, cert *x509.Certificate) string {
	t.Helper()
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))
}
