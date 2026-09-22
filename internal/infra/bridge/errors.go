package bridge

import "errors"

// errNotASessionSubject is returned when a message arrives on a subject the
// session control plane does not recognise. It should be unreachable: the
// only subscriber to sessionRoot is the code that builds those subjects.
var errNotASessionSubject = errors.New("bridge: not a session subject")
