# Go repository conventions

## No comments by default

Write no comments. Identifier names carry meaning. Only add a comment when the WHY is non-obvious: a hidden constraint,
a workaround, a subtle invariant.

## Identifier casing — acronyms use PascalCase, not all-caps

Project-wide override of the Go-stdlib initialism rule.

| Use    | Not    |
|--------|--------|
| `Url`  | `URL`  |
| `Id`   | `ID`   |
| `Html` | `HTML` |
| `Api`  | `API`  |
| `Http` | `HTTP` |
| `Json` | `JSON` |
| `Xml`  | `XML`  |

Applies to both exported and unexported identifiers:

```go
// Yes
type ApiError struct{ ... }
const DefaultBaseUrl = "..."
func (c *Client) DocumentUrl(id string) string { ... }
baseUrl   string
sourceUrl string

// No
type APIError struct{ ... }
const DefaultBaseURL = "..."
func (c *Client) DocumentURL(id string) string { ... }
baseURL   string
sourceURL string
```

What this does **not** change:

* Wire-format JSON tags (`json:"id"`, `json:"url"`) — keep server-side casing.
* Plain English in comments/error messages ("HTML payload", "the API returned…").
* Third-party identifiers (`http.Client`, `url.QueryEscape`) — imported as-is.

Rationale: consistent visual rhythm with neighboring camelCase tokens; avoids
the `HTTPSURLId` readability cliff when two acronyms collide.

## Tests

### Black-box: always use the `_test` package

Put tests for `pkg/foo` in `package foo_test`, not `package foo`. Tests should
exercise the package only through its exported surface. If a test cannot be
written without reaching into unexported state, the API is the thing that
needs to change, not the test.

```go
// Yes
package foo_test

import "example.com/repo/pkg/foo"

// No
package foo // gives the test access to unexported fields/methods
```

### Use `t.Context()` for test contexts

Prefer `t.Context()` over `context.Background()` inside `*testing.T` /
`*testing.B` test bodies. It is automatically cancelled when the test
finishes, so stray goroutines and HTTP calls don't outlive the test.
Reach for `context.Background()` / `context.WithCancel(...)` only when
the test genuinely needs a context whose lifetime is decoupled from the
test (e.g. constructing one before calling `t.Run`, or testing
cancellation semantics).

### No test doubles by default

No mocks, stubs, fakes, or in-process `httptest` servers standing in for real
collaborators. Drive the real code paths against the real dependency
(database, HTTP API, message bus, …). The signal we want from tests is "does
the integrated thing work," not "does the code call the methods we told the
mock to expect."

Exceptions exist (test must be fully hermetic, dependency is destructive or
costs money, third-party service has no sandbox) — in those cases the task
will say so explicitly. Until then: real dependency, real call.

