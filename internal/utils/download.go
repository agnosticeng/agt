package utils

import (
	"context"
	"net/url"
	"os"
	"strconv"

	"github.com/agnosticeng/objstr"
)

func CachedDownload(ctx context.Context, src string, dest string) error {
	srcUrl, err := url.Parse(src)

	if err != nil {
		return err
	}

	if len(srcUrl.Fragment) > 0 {
		q, err := url.ParseQuery(srcUrl.Fragment)

		if err != nil {
			return err
		}

		b, err := strconv.ParseBool(q.Get("disable-cache"))

		if err != nil {
			return err
		}

		if !b {
			if _, err := os.Stat(dest); err == nil {
				return nil
			}
		}
	}

	return objstr.FromContextOrDefault(ctx).Copy(ctx, srcUrl, &url.URL{Scheme: "file", Path: dest})
}
