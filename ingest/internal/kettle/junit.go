// Package kettle ports the row-building rules of Kubernetes test-infra's Kettle
// (kettle/make_json.py, Apache-2.0, pinned at commit 528e0fbf0b38) so that
// QueryDock's data matches what Kettle would have written to BigQuery.
package kettle

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// A second XML declaration (not "<?xml-stylesheet" etc.) anywhere but offset 0.
var lateXMLDecl = regexp.MustCompile(`(?s).<\?xml[ \t\r\n?]`)

// Test is one executed test case. Field presence mirrors Kettle's make_result:
// Failed and FailureText are set only for failures.
type Test struct {
	Name        string  `json:"name" parquet:"name"`
	Time        float64 `json:"time" parquet:"time"`
	Failed      *bool   `json:"failed,omitempty" parquet:"failed,optional"`
	FailureText *string `json:"failure_text,omitempty" parquet:"failure_text,optional"`
}

// ErrMalformed marks a JUnit file that Python's ElementTree would refuse to parse.
// Kettle skips such files (they contribute no tests) rather than failing the build.
var ErrMalformed = errors.New("malformed junit xml")

// ErrBadTime marks a testcase whose time attribute float() would reject. In Kettle
// this raises inside row_for_build, and make_rows drops the whole build.
var ErrBadTime = errors.New("unparsable testcase time")

// node is a minimal ElementTree-like element: Text is the character data before
// the first child element (ElementTree's .text).
type node struct {
	Tag      string
	Attr     map[string]string
	Text     string
	Children []*node
	sawChild bool
}

func parseTree(data []byte) (*node, error) {
	data = bytes.TrimPrefix(data, utf8BOM)
	// expat rejects invalid UTF-8; Go's decoder would silently map it to U+FFFD.
	if !utf8.Valid(data) {
		return nil, ErrMalformed
	}
	// ElementTree only accepts an XML declaration at the very start of the document.
	if lateXMLDecl.Match(data) {
		return nil, ErrMalformed
	}
	dec := xml.NewDecoder(bytes.NewReader(data))
	dec.Strict = true
	dec.CharsetReader = func(label string, r io.Reader) (io.Reader, error) {
		if strings.EqualFold(label, "utf-8") || strings.EqualFold(label, "us-ascii") {
			return r, nil
		}
		return nil, fmt.Errorf("unsupported charset %q", label)
	}
	var root *node
	var stack []*node
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, ErrMalformed
		}
		switch t := tok.(type) {
		case xml.StartElement:
			n := &node{Tag: t.Name.Local, Attr: map[string]string{}}
			for _, a := range t.Attr {
				n.Attr[a.Name.Local] = a.Value
			}
			if len(stack) == 0 {
				if root != nil {
					return nil, ErrMalformed // junk after document element
				}
				root = n
			} else {
				parent := stack[len(stack)-1]
				parent.Children = append(parent.Children, n)
				parent.sawChild = true
			}
			stack = append(stack, n)
		case xml.EndElement:
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if len(stack) > 0 {
				top := stack[len(stack)-1]
				if !top.sawChild {
					top.Text += string(t)
				}
			} else if len(bytes.TrimSpace(t)) > 0 {
				return nil, ErrMalformed // text outside the document element
			}
		}
	}
	if root == nil || len(stack) != 0 {
		return nil, ErrMalformed
	}
	return root, nil
}

func children(n *node, tag string) []*node {
	var out []*node
	for _, c := range n.Children {
		if c.Tag == tag {
			out = append(out, c)
		}
	}
	return out
}

func attrOr(n *node, key, def string) string {
	if v, ok := n.Attr[key]; ok {
		return v
	}
	return def
}

// pyFloat mimics Python's float() for the strings JUnit files contain.
func pyFloat(s string) (float64, error) {
	t := strings.TrimSpace(s)
	t = strings.ReplaceAll(t, "_", "")
	switch strings.ToLower(strings.TrimLeft(t, "+-")) {
	case "inf", "infinity", "nan":
	default:
		if strings.ContainsAny(t, "xXpP") { // Go accepts hex floats, Python does not
			return 0, ErrBadTime
		}
	}
	v, err := strconv.ParseFloat(t, 64)
	if err != nil {
		var ne *strconv.NumError
		if errors.As(err, &ne) && ne.Err == strconv.ErrRange {
			return v, nil // Python returns inf/0.0 on overflow/underflow too
		}
		return 0, ErrBadTime
	}
	return v, nil
}

func parseResult(tc *node) (float64, *string, bool, error) {
	tm := 0.0
	if raw := tc.Attr["time"]; raw != "" { // float(attrib.get('time') or 0)
		v, err := pyFloat(raw)
		if err != nil {
			return 0, nil, false, err
		}
		tm = v
	}
	var failure *string
	for _, f := range children(tc, "failure") { // last <failure> wins, as in Kettle
		msgAttr, hasAttr := f.Attr["message"]
		msgData := f.Text
		var text string
		switch {
		case hasAttr && msgAttr != "" && msgData != "":
			text = msgAttr + "\n" + msgData
		case hasAttr && msgAttr != "":
			text = msgAttr
		case msgData != "":
			text = msgData
		default:
			text = "No Failure Message Found"
		}
		failure = &text
	}
	skipped := len(children(tc, "skipped")) > 0
	return tm, failure, skipped, nil
}

func makeResult(name string, tm float64, failure *string) Test {
	t := Test{Name: name, Time: tm}
	if failure != nil && *failure != "" {
		yes := true
		t.Failed = &yes
		t.FailureText = failure
	}
	return t
}

// ParseJUnit returns the executed (non-skipped) tests in one JUnit file, following
// make_json.parse_junit. A malformed file yields ErrMalformed (Kettle: no tests);
// an unparsable time yields ErrBadTime (Kettle: the build's row is dropped).
func ParseJUnit(data []byte) ([]Test, error) {
	root, err := parseTree(data)
	if err != nil {
		return nil, err
	}
	var out []Test
	switch root.Tag {
	case "testsuite":
		for _, tc := range children(root, "testcase") {
			tm, failure, skipped, err := parseResult(tc)
			if err != nil {
				return nil, err
			}
			if skipped {
				continue
			}
			out = append(out, makeResult(attrOr(tc, "name", "<unspecified>"), tm, failure))
		}
	case "testsuites":
		for _, suite := range root.Children {
			suiteName := attrOr(suite, "name", "<unspecified>")
			for _, tc := range children(suite, "testcase") {
				name := suiteName + " " + attrOr(tc, "name", "<unspecified>")
				tm, failure, skipped, err := parseResult(tc)
				if err != nil {
					return nil, err
				}
				if skipped {
					continue
				}
				out = append(out, makeResult(name, tm, failure))
			}
		}
	}
	// Any other root tag: Kettle logs an error and yields nothing.
	return out, nil
}
