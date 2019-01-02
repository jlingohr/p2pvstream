package stringutil

import "path/filepath"

func RemoveFilenameExt(fname string) string {
	extension := filepath.Ext(fname)
	noExtFilename := fname[0 : len(fname)-len(extension)]

	return noExtFilename
}
