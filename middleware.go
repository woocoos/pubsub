package pubsub

var defaults = []Middleware{
	RecoveryMiddleware{},
}
