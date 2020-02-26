package util_test

import (
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStringListContains(t *testing.T) {
	list := []string{"apple", "orange", "banana"}
	assert.True(t, util.StringListContains(list, "orange"))
	assert.False(t, util.StringListContains(list, "wedgie"))
	// Don't crash on nil list
	assert.False(t, util.StringListContains(nil, "mars"))
}

func TestStringListContainsAll(t *testing.T) {
	list1 := []string{"apple", "orange", "banana"}
	list2 := []string{"apple", "orange", "banana"}
	list3 := []string{"apple", "orange", "fig"}

	assert.True(t, util.StringListContainsAll(list1, list2))
	assert.False(t, util.StringListContainsAll(list1, list3))
}

func TestAlgorithmFromManifestName(t *testing.T) {
	names := map[string]string{
		"manifest-md5.txt":       "md5",
		"tagmanifest-sha256.txt": "sha256",
		"manifest-sha512.txt":    "sha512",
	}
	for filename, algorithm := range names {
		alg, err := util.AlgorithmFromManifestName(filename)
		assert.Nil(t, err)
		assert.Equal(t, algorithm, alg)
	}
	_, err := util.AlgorithmFromManifestName("bad-file-name.txt")
	assert.NotNil(t, err)
}

func TestLooksLikeManifest(t *testing.T) {
	assert.True(t, util.LooksLikeManifest("manifest-md5.txt"))
	assert.True(t, util.LooksLikeManifest("manifest-sha256.txt"))
	// No: is tag manifest
	assert.False(t, util.LooksLikeManifest("tagmanifest-md5.txt"))
	// No: is tag file
	assert.False(t, util.LooksLikeManifest("bag-info.txt"))
	// No: is payload file
	assert.False(t, util.LooksLikeManifest("data/manifest-sha256.txt"))
}

func TestLooksLikeTagManifest(t *testing.T) {
	assert.True(t, util.LooksLikeTagManifest("tagmanifest-md5.txt"))
	assert.True(t, util.LooksLikeTagManifest("tagmanifest-sha256.txt"))
	// No: is manifest
	assert.False(t, util.LooksLikeTagManifest("manifest-md5.txt"))
	// No: is tag file
	assert.False(t, util.LooksLikeTagManifest("bag-info.txt"))
	// No: is payload file
	assert.False(t, util.LooksLikeTagManifest("data/manifest-sha256.txt"))
}

func TestContainsControlCharacter(t *testing.T) {
	assert.True(t, util.ContainsControlCharacter("\u0000 -- NULL"))
	assert.True(t, util.ContainsControlCharacter("\u0001 -- START OF HEADING"))
	assert.True(t, util.ContainsControlCharacter("\u0002 -- START OF TEXT"))
	assert.True(t, util.ContainsControlCharacter("\u0003 -- END OF TEXT"))
	assert.True(t, util.ContainsControlCharacter("\u0004 -- END OF TRANSMISSION"))
	assert.True(t, util.ContainsControlCharacter("\u0005 -- ENQUIRY"))
	assert.True(t, util.ContainsControlCharacter("\u0006 -- ACKNOWLEDGE"))
	assert.True(t, util.ContainsControlCharacter("\u0007 -- BELL"))
	assert.True(t, util.ContainsControlCharacter("\u0008 -- BACKSPACE"))
	assert.True(t, util.ContainsControlCharacter("\u0009 -- CHARACTER TABULATION"))
	assert.True(t, util.ContainsControlCharacter("\u000A -- LINE FEED (LF)"))
	assert.True(t, util.ContainsControlCharacter("\u000B -- LINE TABULATION"))
	assert.True(t, util.ContainsControlCharacter("\u000C -- FORM FEED (FF)"))
	assert.True(t, util.ContainsControlCharacter("\u000D -- CARRIAGE RETURN (CR)"))
	assert.True(t, util.ContainsControlCharacter("\u000E -- SHIFT OUT"))
	assert.True(t, util.ContainsControlCharacter("\u000F -- SHIFT IN"))
	assert.True(t, util.ContainsControlCharacter("\u0010 -- DATA LINK ESCAPE"))
	assert.True(t, util.ContainsControlCharacter("\u0011 -- DEVICE CONTROL ONE"))
	assert.True(t, util.ContainsControlCharacter("\u0012 -- DEVICE CONTROL TWO"))
	assert.True(t, util.ContainsControlCharacter("\u0013 -- DEVICE CONTROL THREE"))
	assert.True(t, util.ContainsControlCharacter("\u0014 -- DEVICE CONTROL FOUR"))
	assert.True(t, util.ContainsControlCharacter("\u0015 -- NEGATIVE ACKNOWLEDGE"))
	assert.True(t, util.ContainsControlCharacter("\u0016 -- SYNCHRONOUS IDLE"))
	assert.True(t, util.ContainsControlCharacter("\u0017 -- END OF TRANSMISSION BLOCK"))
	assert.True(t, util.ContainsControlCharacter("\u0018 -- CANCEL"))
	assert.True(t, util.ContainsControlCharacter("\u0019 -- END OF MEDIUM"))
	assert.True(t, util.ContainsControlCharacter("\u001A -- SUBSTITUTE"))
	assert.True(t, util.ContainsControlCharacter("\u001B -- ESCAPE"))
	assert.True(t, util.ContainsControlCharacter("\u001C -- INFORMATION SEPARATOR FOUR"))
	assert.True(t, util.ContainsControlCharacter("\u001D -- INFORMATION SEPARATOR THREE"))
	assert.True(t, util.ContainsControlCharacter("\u001E -- INFORMATION SEPARATOR TWO"))
	assert.True(t, util.ContainsControlCharacter("\u001F -- INFORMATION SEPARATOR ONE"))
	assert.True(t, util.ContainsControlCharacter("\u007F -- DELETE"))
	assert.True(t, util.ContainsControlCharacter("\u0080 -- <control>"))
	assert.True(t, util.ContainsControlCharacter("\u0081 -- <control>"))
	assert.True(t, util.ContainsControlCharacter("\u0082 -- BREAK PERMITTED HERE"))
	assert.True(t, util.ContainsControlCharacter("\u0083 -- NO BREAK HERE"))
	assert.True(t, util.ContainsControlCharacter("\u0084 -- <control>"))
	assert.True(t, util.ContainsControlCharacter("\u0085 -- NEXT LINE (NEL)"))
	assert.True(t, util.ContainsControlCharacter("\u0086 -- START OF SELECTED AREA"))
	assert.True(t, util.ContainsControlCharacter("\u0087 -- END OF SELECTED AREA"))
	assert.True(t, util.ContainsControlCharacter("\u0088 -- CHARACTER TABULATION SET"))
	assert.True(t, util.ContainsControlCharacter("\u0089 -- CHARACTER TABULATION WITH JUSTIFICATION"))
	assert.True(t, util.ContainsControlCharacter("\u008A -- LINE TABULATION SET"))
	assert.True(t, util.ContainsControlCharacter("\u008B -- PARTIAL LINE FORWARD"))
	assert.True(t, util.ContainsControlCharacter("\u008C -- PARTIAL LINE BACKWARD"))
	assert.True(t, util.ContainsControlCharacter("\u008D -- REVERSE LINE FEED"))
	assert.True(t, util.ContainsControlCharacter("\u008E -- SINGLE SHIFT TWO"))
	assert.True(t, util.ContainsControlCharacter("\u008F -- SINGLE SHIFT THREE"))
	assert.True(t, util.ContainsControlCharacter("\u0090 -- DEVICE CONTROL STRING"))
	assert.True(t, util.ContainsControlCharacter("\u0091 -- PRIVATE USE ONE"))
	assert.True(t, util.ContainsControlCharacter("\u0092 -- PRIVATE USE TWO"))
	assert.True(t, util.ContainsControlCharacter("\u0093 -- SET TRANSMIT STATE"))
	assert.True(t, util.ContainsControlCharacter("\u0094 -- CANCEL CHARACTER"))
	assert.True(t, util.ContainsControlCharacter("\u0095 -- MESSAGE WAITING"))
	assert.True(t, util.ContainsControlCharacter("\u0096 -- START OF GUARDED AREA"))
	assert.True(t, util.ContainsControlCharacter("\u0097 -- END OF GUARDED AREA"))
	assert.True(t, util.ContainsControlCharacter("\u0098 -- START OF STRING"))
	assert.True(t, util.ContainsControlCharacter("\u0099 -- <control>"))
	assert.True(t, util.ContainsControlCharacter("\u009A -- SINGLE CHARACTER INTRODUCER"))
	assert.True(t, util.ContainsControlCharacter("\u009B -- CONTROL SEQUENCE INTRODUCER"))
	assert.True(t, util.ContainsControlCharacter("\u009C -- STRING TERMINATOR"))
	assert.True(t, util.ContainsControlCharacter("\u009D -- OPERATING SYSTEM COMMAND"))
	assert.True(t, util.ContainsControlCharacter("\u009E -- PRIVACY MESSAGE"))
	assert.True(t, util.ContainsControlCharacter("\u009F -- APPLICATION PROGRAM COMMAND"))
	assert.True(t, util.ContainsControlCharacter("data/datastream\u007f.txt"))

	assert.False(t, util.ContainsControlCharacter("./this/is/a/valid/file/name.txt"))
}

func TestContainsEscapedControl(t *testing.T) {
	assert.True(t, util.ContainsEscapedControl("\\u0000 -- NULL"))
	assert.True(t, util.ContainsEscapedControl("\\u0001 -- START OF HEADING"))
	assert.True(t, util.ContainsEscapedControl("\\u0002 -- START OF TEXT"))
	assert.True(t, util.ContainsEscapedControl("\\u0003 -- END OF TEXT"))
	assert.True(t, util.ContainsEscapedControl("\\u0004 -- END OF TRANSMISSION"))
	assert.True(t, util.ContainsEscapedControl("\\u0005 -- ENQUIRY"))
	assert.True(t, util.ContainsEscapedControl("\\u0006 -- ACKNOWLEDGE"))
	assert.True(t, util.ContainsEscapedControl("\\u0007 -- BELL"))
	assert.True(t, util.ContainsEscapedControl("\\u0008 -- BACKSPACE"))
	assert.True(t, util.ContainsEscapedControl("\\u0009 -- CHARACTER TABULATION"))
	assert.True(t, util.ContainsEscapedControl("\\u000A -- LINE FEED (LF)"))
	assert.True(t, util.ContainsEscapedControl("\\u000B -- LINE TABULATION"))
	assert.True(t, util.ContainsEscapedControl("\\u000C -- FORM FEED (FF)"))
	assert.True(t, util.ContainsEscapedControl("\\u000D -- CARRIAGE RETURN (CR)"))
	assert.True(t, util.ContainsEscapedControl("\\u000E -- SHIFT OUT"))
	assert.True(t, util.ContainsEscapedControl("\\u000F -- SHIFT IN"))
	assert.True(t, util.ContainsEscapedControl("\\u0010 -- DATA LINK ESCAPE"))
	assert.True(t, util.ContainsEscapedControl("\\u0011 -- DEVICE CONTROL ONE"))
	assert.True(t, util.ContainsEscapedControl("\\u0012 -- DEVICE CONTROL TWO"))
	assert.True(t, util.ContainsEscapedControl("\\u0013 -- DEVICE CONTROL THREE"))
	assert.True(t, util.ContainsEscapedControl("\\u0014 -- DEVICE CONTROL FOUR"))
	assert.True(t, util.ContainsEscapedControl("\\u0015 -- NEGATIVE ACKNOWLEDGE"))
	assert.True(t, util.ContainsEscapedControl("\\u0016 -- SYNCHRONOUS IDLE"))
	assert.True(t, util.ContainsEscapedControl("\\u0017 -- END OF TRANSMISSION BLOCK"))
	assert.True(t, util.ContainsEscapedControl("\\u0018 -- CANCEL"))
	assert.True(t, util.ContainsEscapedControl("\\u0019 -- END OF MEDIUM"))
	assert.True(t, util.ContainsEscapedControl("\\u001A -- SUBSTITUTE"))
	assert.True(t, util.ContainsEscapedControl("\\u001B -- ESCAPE"))
	assert.True(t, util.ContainsEscapedControl("\\u001C -- INFORMATION SEPARATOR FOUR"))
	assert.True(t, util.ContainsEscapedControl("\\u001D -- INFORMATION SEPARATOR THREE"))
	assert.True(t, util.ContainsEscapedControl("\\u001E -- INFORMATION SEPARATOR TWO"))
	assert.True(t, util.ContainsEscapedControl("\\u001F -- INFORMATION SEPARATOR ONE"))
	assert.True(t, util.ContainsEscapedControl("\\u007F -- DELETE"))
	assert.True(t, util.ContainsEscapedControl("\\u0080 -- <control>"))
	assert.True(t, util.ContainsEscapedControl("\\u0081 -- <control>"))
	assert.True(t, util.ContainsEscapedControl("\\u0082 -- BREAK PERMITTED HERE"))
	assert.True(t, util.ContainsEscapedControl("\\u0083 -- NO BREAK HERE"))
	assert.True(t, util.ContainsEscapedControl("\\u0084 -- <control>"))
	assert.True(t, util.ContainsEscapedControl("\\u0085 -- NEXT LINE (NEL)"))
	assert.True(t, util.ContainsEscapedControl("\\u0086 -- START OF SELECTED AREA"))
	assert.True(t, util.ContainsEscapedControl("\\u0087 -- END OF SELECTED AREA"))
	assert.True(t, util.ContainsEscapedControl("\\u0088 -- CHARACTER TABULATION SET"))
	assert.True(t, util.ContainsEscapedControl("\\u0089 -- CHARACTER TABULATION WITH JUSTIFICATION"))
	assert.True(t, util.ContainsEscapedControl("\\u008A -- LINE TABULATION SET"))
	assert.True(t, util.ContainsEscapedControl("\\u008B -- PARTIAL LINE FORWARD"))
	assert.True(t, util.ContainsEscapedControl("\\u008C -- PARTIAL LINE BACKWARD"))
	assert.True(t, util.ContainsEscapedControl("\\u008D -- REVERSE LINE FEED"))
	assert.True(t, util.ContainsEscapedControl("\\u008E -- SINGLE SHIFT TWO"))
	assert.True(t, util.ContainsEscapedControl("\\u008F -- SINGLE SHIFT THREE"))
	assert.True(t, util.ContainsEscapedControl("\\u0090 -- DEVICE CONTROL STRING"))
	assert.True(t, util.ContainsEscapedControl("\\u0091 -- PRIVATE USE ONE"))
	assert.True(t, util.ContainsEscapedControl("\\u0092 -- PRIVATE USE TWO"))
	assert.True(t, util.ContainsEscapedControl("\\u0093 -- SET TRANSMIT STATE"))
	assert.True(t, util.ContainsEscapedControl("\\u0094 -- CANCEL CHARACTER"))
	assert.True(t, util.ContainsEscapedControl("\\u0095 -- MESSAGE WAITING"))
	assert.True(t, util.ContainsEscapedControl("\\u0096 -- START OF GUARDED AREA"))
	assert.True(t, util.ContainsEscapedControl("\\u0097 -- END OF GUARDED AREA"))
	assert.True(t, util.ContainsEscapedControl("\\u0098 -- START OF STRING"))
	assert.True(t, util.ContainsEscapedControl("\\u0099 -- <control>"))
	assert.True(t, util.ContainsEscapedControl("\\u009A -- SINGLE CHARACTER INTRODUCER"))
	assert.True(t, util.ContainsEscapedControl("\\u009B -- CONTROL SEQUENCE INTRODUCER"))
	assert.True(t, util.ContainsEscapedControl("\\u009C -- STRING TERMINATOR"))
	assert.True(t, util.ContainsEscapedControl("\\u009D -- OPERATING SYSTEM COMMAND"))
	assert.True(t, util.ContainsEscapedControl("\\u009E -- PRIVACY MESSAGE"))
	assert.True(t, util.ContainsEscapedControl("\\u009F -- APPLICATION PROGRAM COMMAND"))
	assert.True(t, util.ContainsEscapedControl("data/datastream\\u007f.txt"))

	assert.False(t, util.ContainsEscapedControl("./this/is/a/valid/file/name.txt"))
}

func TestUCFirst(t *testing.T) {
	assert.Equal(t, "Institution", util.UCFirst("institution"))
	assert.Equal(t, "Institution", util.UCFirst("INSTITUTION"))
	assert.Equal(t, "Institution", util.UCFirst("inStiTuTioN"))
}
