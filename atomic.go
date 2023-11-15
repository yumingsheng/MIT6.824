package mr

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Dump content in io.Reader into filename. Used in doMapTask
// and doReduceTask in worker.
func atomicWriteFile(filename string, r io.Reader) (err error) {
	// Write to a tmp file first, then we'll actomically
	// replace the target file with the tmp file.
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}

	f, err := ioutil.TempFile(dir, file)
	if err != nil {
		return fmt.Errorf("cannot create tmp file: %v", err)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(f.Name())
		}
	}()
	defer f.Close()
	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("cannot write data to tempfile %q: %v", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cannot close tempfile %q: %v", name, err)
	}

	// Get the file mode from the original file and use that
	// for the replacement file too.
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		// no original file
	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(name, info.Mode()); err != nil {
			return fmt.Errorf("cannot set filemode on tempfile %q: %v", name, err)
		}
	}
	if err := os.Rename(name, filename); err != nil {
		return fmt.Errorf("cannot replace %q with tempfile %q: %v", filename, name, err)
	}
	return nil
}