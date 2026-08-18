// Package redisqueue provides reliable timer persistence primitives using Redis
// sorted sets and hashes. Applications can wrap Queue to supply tenant discovery
// and implement timer/engine.Queue.
package redisqueue
