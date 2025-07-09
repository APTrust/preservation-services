package service

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PosixMetadata contains POSIX file metadata such as owner, group,
// modtime, mode (permissions), etc. parsed from tar file headers.
// This data may or may not be present when we parse a tar file.
type PosixMetadata struct {
	Uid          int       `json:"uid"`
	Gid          int       `json:"gid"`
	Uname        string    `json:"uname"`
	Gname        string    `json:"gname"`
	AccessTime   time.Time `json:"atime"`
	CreationTime time.Time `json:"ctime"`
	ModTime      time.Time `json:"mtime"`
	Mode         int64     `json:"mode"`
}

func PosixMetadataFromHeaderString(s string) (*PosixMetadata, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 8 {
		return nil, fmt.Errorf("invalid posix metadata header string")
	}
	uid, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid uid in posix metadata header string")
	}
	gid, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid gid in posix metadata header string")
	}
	mode, err := strconv.ParseInt(parts[7], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid mode in posix metadata header string")
	}
	atimeUnix, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid access time in posix metadata header string")
	}
	ctimeUnix, err := strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid creation time in posix metadata header string")
	}
	mtimeUnix, err := strconv.ParseInt(parts[6], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid mod time in posix metadata header string")
	}
	metadata := &PosixMetadata{
		Uid:          uid,
		Gid:          gid,
		Uname:        parts[2],
		Gname:        parts[3],
		AccessTime:   time.Unix(atimeUnix, 0),
		CreationTime: time.Unix(ctimeUnix, 0),
		ModTime:      time.Unix(mtimeUnix, 0),
		Mode:         mode,
	}
	return metadata, nil
}

// ToHeaderString returns a colon-delimited string of POSIX metadata
// in the format uid:gid:uname:gname:atime:ctime:mtime:mode.
// This string can be sent to S3 as a metadata header and stored
// with the S3 object.
func (pm *PosixMetadata) ToHeaderString() string {
	return fmt.Sprintf("%d:%d:%s:%s:%d:%d:%d:%d",
		pm.Uid,
		pm.Gid,
		pm.Uname,
		pm.Gname,
		UnixTimeOrZero(pm.AccessTime),
		UnixTimeOrZero(pm.CreationTime),
		UnixTimeOrZero(pm.ModTime),
		pm.Mode,
	)
}

// UnixTimeOrZero returns time t as a Unix int64 timestamp.
// If t is empty/zero, this returns zero instead of
// -62135596800, which is a representation that makes no
// sense on most POSIX systems. Zero is good enough to tell
// us we didn't get a timestamp from the tar header.
func UnixTimeOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}
