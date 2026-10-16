package core

import "errors"

var (
	ErrInstanceNotFound    = errors.New("instance not found")
	ErrNodeNotFound        = errors.New("node not found")
	ErrInvalidTransition   = errors.New("invalid state transition")
	ErrNoValidNode         = errors.New("no valid node")
	ErrRequestRejected     = errors.New("request rejected by scheduler")
	ErrCapacityConflict    = errors.New("capacity conflict")
	ErrInvalidRequest      = errors.New("invalid request")
	ErrFlavorNotFound      = errors.New("flavor not found")
	ErrVolumeNotFound      = errors.New("volume not found")
	ErrUnsupportedPlatform = errors.New("hypervisor not available on this platform")
)
