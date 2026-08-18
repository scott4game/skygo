package timer

import "time"

// Timer is the common lifecycle for an ID-based scheduler.
type Timer interface {
	// Add schedules an ID at expireTime.
	Add(id uint64, expireTime time.Time) error

	// Remove cancels an ID.
	Remove(id uint64) error

	// Reschedule changes an ID's deadline.
	Reschedule(id uint64, newExpireTime time.Time) error

	// Start begins processing.
	Start()

	// Stop ends processing.
	Stop()
}
