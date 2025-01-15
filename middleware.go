package pubsub

var defaults = []Middleware{
	LoggerMiddleware{},
	RecoveryMiddleware{},
}
