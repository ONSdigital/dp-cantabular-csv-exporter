package request

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPatch(t *testing.T) {

	Convey("Validating a valid patch with a supported op and array of strings value is successful", t, func() {
		patch := Patch{
			Op:    "add",
			Path:  "/a/b/c",
			Value: []string{"foo"},
		}
		So(patch.Validate(OpAdd), ShouldBeNil)
	})

	Convey("Validating a valid patch with a supported op and float64 value is successful", t, func() {
		patch := Patch{
			Op:    "add",
			Path:  "/a/b/c",
			Value: float64(123.321),
		}
		So(patch.Validate(OpAdd), ShouldBeNil)
	})

	Convey("Validating a patch struct with an invalid op fails with the expected error", t, func() {
		patch := Patch{
			Op:    "wrong",
			Path:  "/a/b/c",
			Value: []string{"foo"},
		}
		So(patch.Validate(), ShouldResemble, ErrInvalidOp)
	})

	Convey("Validating a valid patch with an unsupported op fails with the expected error", t, func() {
		patch := Patch{
			Op:    "add",
			Path:  "/a/b/c",
			Value: []string{"foo"},
		}
		So(patch.Validate(OpRemove), ShouldResemble, ErrUnsupportedOp("add", []PatchOp{OpRemove}))
	})

	Convey("Validating a patch struct with missing members for an operation results in the expected error being returned", t, func() {
		patch := Patch{
			Op:   "add",
			Path: "/a/b/c",
		}
		So(patch.Validate(OpAdd), ShouldResemble, ErrMissingMember([]string{"value"}))
		patch = Patch{
			Op:    "replace",
			Value: []string{"foo"},
		}
		So(patch.Validate(OpReplace), ShouldResemble, ErrMissingMember([]string{"path"}))
		patch = Patch{
			Op: "test",
		}
		So(patch.Validate(OpTest), ShouldResemble, ErrMissingMember([]string{"path", "value"}))
		patch = Patch{
			Op: "remove",
		}
		So(patch.Validate(OpRemove), ShouldResemble, ErrMissingMember([]string{"path"}))
		patch = Patch{
			Op:   "move",
			Path: "/a/b/c",
		}
		So(patch.Validate(OpMove), ShouldResemble, ErrMissingMember([]string{"from"}))
		patch = Patch{
			Op:   "copy",
			From: "/c/b/a",
		}
		So(patch.Validate(OpCopy), ShouldResemble, ErrMissingMember([]string{"path"}))
	})

}
