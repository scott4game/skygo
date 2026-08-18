package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestReadWrite(t *testing.T) {
	payload := []byte{1, 2, 3, 4, 5}
	var buf bytes.Buffer
	if err := Write(&buf, payload); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %v want %v", got, payload)
	}
}

func TestConstants(t *testing.T) {
	if HeadLen != 4 {
		t.Fatalf("HeadLen = %d", HeadLen)
	}
	if MaxPayload != 4*1024*1024 {
		t.Fatal("MaxPayload mismatch")
	}
}

func TestRead_TooLargeLength(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, MaxPayload+1)
	_, err := Read(&buf)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRead_ShortPayload(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(100))
	buf.WriteByte(1) // only 1 byte, need 100
	_, err := Read(&buf)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRead_EmptyReader(t *testing.T) {
	var buf bytes.Buffer
	_, err := Read(&buf)
	if err == nil {
		t.Fatal("expected EOF")
	}
}

func TestWrite_EmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, nil); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

func TestWriteRejectsPayloadAboveReadLimit(t *testing.T) {
	payload := make([]byte, int(MaxPayload)+1)
	if err := Write(io.Discard, payload); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("Write error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestWriteCompletesShortWrites(t *testing.T) {
	var dst bytes.Buffer
	w := &shortWriter{dst: &dst, max: 2}
	if err := Write(w, []byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	want := append([]byte{0, 0, 0, 5}, []byte("hello")...)
	if !bytes.Equal(dst.Bytes(), want) {
		t.Fatalf("wire bytes = %v, want %v", dst.Bytes(), want)
	}
}

func TestWriteRejectsNoProgress(t *testing.T) {
	if err := Write(zeroWriter{}, []byte("x")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Write error = %v, want io.ErrShortWrite", err)
	}
}

type shortWriter struct {
	dst *bytes.Buffer
	max int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.max {
		p = p[:w.max]
	}
	return w.dst.Write(p)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }
