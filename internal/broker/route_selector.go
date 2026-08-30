package broker

import (
	"fmt"
	"strings"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// orderedHeldRoutes returns the held set with output first and every other
// route in claim order. It is the shared ordering for queue drains and
// keyboard-capability selection.
func orderedHeldRoutes(stub *Stub) []RouteKey {
	if stub == nil {
		return nil
	}
	routes, output := stub.RouteSnapshot()
	if output == nil {
		return routes
	}
	ordered := make([]RouteKey, 0, len(routes))
	ordered = append(ordered, *output)
	for _, route := range routes {
		if route != *output {
			ordered = append(ordered, route)
		}
	}
	return ordered
}

func sameRouteOrder(a, b []RouteKey) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameRouteSet(a, b []RouteKey) bool {
	if len(a) != len(b) {
		return false
	}
	want := make(map[RouteKey]struct{}, len(a))
	for _, route := range a {
		want[route] = struct{}{}
	}
	for _, route := range b {
		if _, ok := want[route]; !ok {
			return false
		}
	}
	return true
}

func sessionAttachmentMatchesStub(attachment mappings.SessionAttachment, stub *Stub) bool {
	refs := routeRefsFromSessionAttachment(attachment)
	stored := make([]RouteKey, 0, len(refs))
	for _, ref := range refs {
		stored = append(stored, routeKeyFromRef(ref))
	}
	routes, output := stub.RouteSnapshot()
	if !sameRouteSet(stored, routes) {
		return false
	}
	storedOutput := routeKeyFromSessionAttachment(attachment)
	return output != nil && *output == storedOutput
}

// resolveHeldRoute is the trust boundary for caller-supplied route selectors.
// A selector may name a held channel or a held Telegram topic. Exactly one held
// route must match; unknown and ambiguous values fail closed and never fall
// back to the output route.
func (b *Broker) resolveHeldRoute(stub *Stub, selector string) (RouteKey, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return RouteKey{}, fmt.Errorf("route selector is empty")
	}
	if stub == nil {
		return RouteKey{}, fmt.Errorf("route %q is not held by this session", selector)
	}
	var matches []RouteKey
	for _, route := range stub.Routes() {
		matched := strings.EqualFold(route.Channel, selector)
		if !matched && route.Channel == "telegram" {
			ref := b.routeRefForKey(route)
			matched = ref.Name != "" && strings.EqualFold(ref.Name, selector)
		}
		if matched {
			matches = append(matches, route)
		}
	}
	if len(matches) == 0 {
		return RouteKey{}, fmt.Errorf("route %q is not held by this session", selector)
	}
	if len(matches) > 1 {
		return RouteKey{}, fmt.Errorf("route selector %q is ambiguous across held routes", selector)
	}
	return matches[0], nil
}

// resolveToolRoute validates and removes the dormant `channel` selector before
// channel dispatch. Raw chat_id/topic_id remain in args so the existing
// rejectDestinationOverride gate continues to reject them on presence.
func (b *Broker) resolveToolRoute(stub *Stub, args map[string]any) (RouteKey, map[string]any, error) {
	if stub == nil {
		return RouteKey{}, nil, fmt.Errorf("tool_call before attach: no route claimed")
	}
	value, selected := args["channel"]
	var route RouteKey
	if selected {
		selector, ok := value.(string)
		if !ok {
			return RouteKey{}, nil, fmt.Errorf("channel must be a held-route selector string")
		}
		resolved, err := b.resolveHeldRoute(stub, selector)
		if err != nil {
			return RouteKey{}, nil, err
		}
		route = resolved
	} else {
		output := stub.OutputRoute()
		if output == nil || !stub.HasRoute(*output) {
			return RouteKey{}, nil, fmt.Errorf("tool_call before attach: no route claimed")
		}
		route = *output
	}
	holder, held := b.Routes.Holder(route)
	if !held || holder != stub {
		return RouteKey{}, nil, fmt.Errorf("selected route is no longer held by this session")
	}
	forwarded := make(map[string]any, len(args))
	for key, value := range args {
		if key != "channel" {
			forwarded[key] = value
		}
	}
	return route, forwarded, nil
}

func (b *Broker) routeRefForKey(key RouteKey) mappings.RouteRef {
	ref := mappings.RouteRef{Channel: key.Channel, ChatID: key.ChatID}
	if key.HasTopic {
		topicID := key.TopicID
		ref.TopicID = &topicID
		if topic, ok := b.Mappings().LookupTopicByID(key.Channel, key.ChatID, key.TopicID); ok {
			ref.Name = topic.Name
			ref.Group = topic.Group
		} else {
			ref.Name = fmt.Sprintf("topic-%d", key.TopicID)
		}
	} else {
		ref.Name = nonTopicRouteName(key.Channel)
	}
	return ref
}

func routeKeyFromRef(ref mappings.RouteRef) RouteKey {
	return MakeRouteKey(ref.Channel, ref.ChatID, ref.TopicID)
}

func (b *Broker) routeSetRefs(stub *Stub) ([]mappings.RouteRef, *mappings.RouteRef) {
	routes, output := stub.RouteSnapshot()
	refs := make([]mappings.RouteRef, 0, len(routes))
	for _, route := range routes {
		refs = append(refs, b.routeRefForKey(route))
	}
	var outputRef *mappings.RouteRef
	if output != nil {
		ref := b.routeRefForKey(*output)
		outputRef = &ref
	}
	return refs, outputRef
}

func (b *Broker) withRouteSet(stub *Stub, msg ipc.AttachedMsg) ipc.AttachedMsg {
	routes, output := b.routeSetRefs(stub)
	msg.Routes = routes
	msg.Output = output
	return msg
}

func (b *Broker) routeLabel(key RouteKey) string {
	ref := b.routeRefForKey(key)
	if ref.Name == "" || strings.EqualFold(ref.Name, ref.Channel) {
		return ref.Channel
	}
	return ref.Channel + " · " + ref.Name
}
