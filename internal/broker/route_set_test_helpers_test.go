package broker

// bindOutputRouteForTest models a route-table-independent stub binding in
// focused unit tests. It deliberately does not confirm the route; tests that
// exercise destructive consumption must call confirmOutputRouteForTest.
func bindOutputRouteForTest(stub *Stub, route *RouteKey) {
	if route == nil {
		stub.ClearRoutes()
		return
	}
	stub.AddRoute(*route)
	stub.SetOutputRoute(*route)
}

func confirmOutputRouteForTest(stub *Stub) {
	if output := stub.OutputRoute(); output != nil {
		stub.MarkRouteConfirmed(*output)
	}
}

func outputRouteConfirmedForTest(stub *Stub) bool {
	output := stub.OutputRoute()
	return output != nil && stub.RouteConfirmed(*output)
}
