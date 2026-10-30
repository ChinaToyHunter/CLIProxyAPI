package helps

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// NormalizeXAIResponseEvent accepts the legacy terminal spelling without turning
// an incomplete or failed response into a successful completion.
func NormalizeXAIResponseEvent(data []byte) []byte {
	if gjson.GetBytes(data, "type").String() != "response.done" {
		return data
	}
	kind := "response.completed"
	switch gjson.GetBytes(data, "response.status").String() {
	case "incomplete":
		kind = "response.incomplete"
	case "failed":
		kind = "response.failed"
	}
	out, errSet := sjson.SetBytes(data, "type", kind)
	if errSet != nil {
		return data
	}
	return out
}
