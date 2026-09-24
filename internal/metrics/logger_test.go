package metrics

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

func TestInfo_WritesInfoPrefixedMessage(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	Info("test message")

	out := buf.String()
	if !strings.Contains(out, "[INFO]") {
		t.Errorf("output %q does not contain [INFO]", out)
	}
	if !strings.Contains(out, "test message") {
		t.Errorf("output %q does not contain the logged message", out)
	}
}

func TestError_WritesErrorPrefixedMessage(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	Error("test failure")

	out := buf.String()
	if !strings.Contains(out, "[ERROR]") {
		t.Errorf("output %q does not contain [ERROR]", out)
	}
	if !strings.Contains(out, "test failure") {
		t.Errorf("output %q does not contain the logged message", out)
	}
}
