package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type syncSpec struct {
	schedule string
	region   string
	bucket   string
	prefix   string
	dst      string
	onStart  bool
}

func (s *syncSpec) toCSV() (string, error) {
	var record []string
	if s.schedule != "" {
		record = append(record, "schedule="+s.schedule)
	}
	if s.region != "" {
		record = append(record, "region="+s.region)
	}
	record = append(record, "bucket="+s.bucket)
	record = append(record, "prefix="+s.prefix)
	record = append(record, "dst="+s.dst)
	record = append(record, fmt.Sprintf("on-start=%t", s.onStart))

	var b bytes.Buffer
	w := csv.NewWriter(&b)
	if err := w.Write(record); err != nil {
		return "", err
	}
	w.Flush()

	return strings.TrimSuffix(b.String(), "\n"), nil
}

func (s *syncSpec) fromCSV(str string) error {
	r := csv.NewReader(strings.NewReader(str))
	fields, err := r.Read()
	if err != nil && err != io.EOF {
		return err
	}

	for _, field := range fields {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) == 2 {
			key, value := parts[0], parts[1]
			switch key {
			case "schedule":
				s.schedule = value
			case "region":
				s.region = value
			case "bucket":
				s.bucket = value
			case "prefix":
				s.prefix = value
			case "dst":
				s.dst = value
			case "on-start":
				if s.onStart, err = strconv.ParseBool(value); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unexpected key '%s' in '%s'", key, field)
			}
		} else {
			return fmt.Errorf("invalid field '%s' must be a key=value pair", field)
		}
	}

	return nil
}

type syncValue struct {
	specs []*syncSpec
}

// String implements flag.Value
func (v *syncValue) String() string {
	var buf bytes.Buffer

	for _, s := range v.specs {
		if buf.Len() > 0 {
			buf.WriteByte(' ')
		}
		str, err := s.toCSV()
		if err != nil {
			return ""
		}
		buf.WriteString(str)
	}

	return buf.String()
}

// Set implements flag.Value
func (v *syncValue) Set(value string) error {
	var s syncSpec
	if err := s.fromCSV(value); err != nil {
		return err
	}

	if s.bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if s.prefix == "" {
		return fmt.Errorf("prefix is required")
	}
	if s.dst == "" {
		return fmt.Errorf("dst is required")
	}

	v.specs = append(v.specs, &s)

	return nil
}

type imageTagValue []string

// String implements flag.Value
func (v *imageTagValue) String() string {
	return strings.Join(*v, ",")
}

// Set implements flag.Value
func (v *imageTagValue) Set(value string) error {
	*v = append(*v, value)
	return nil
}

var (
	oneshot     bool
	stopTimeout time.Duration
	syncFlag    syncValue
	tags        imageTagValue
)

func init() {
	flag.Var(&tags, "image-tag", "Tag of a container image to build and push to a registry after sync.")
	flag.BoolVar(&oneshot, "oneshot", false, "Run the sync and exit.")
	flag.DurationVar(&stopTimeout, "stop-timeout", 10*time.Second, "Timeout in seconds to stop.")
	flag.Var(&syncFlag, "sync", "Sync directories and S3 prefixes.")
}

func main() {
	flag.Parse()

	if len(syncFlag.specs) == 0 {
		log.Println("-sync flag is required")
		os.Exit(1)
	}

	runner, err := newRunner(syncFlag.specs, tags, oneshot, stopTimeout)
	if err != nil {
		log.Fatal(err)
	}

	if err := runner.run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
