// +build formats

package util_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

//
//
// NOTE: These tests are known to fail. See the comments below on which
//       types FIDO can identify and which types the static lookup map
//       identifies. Between the two, they can identify all formats, which
//       means our ingest process will identify them all, because it runs
//       both types of format identification.
//
//       To run these tests, use `./scripts/test.rb units --formats`
//
//       We run these tests only when we want to check new versions of FIDO,
//       new versions of the PRONOM registry files, or updates to the
//       static file type lookup map at constants/mime_types.go.
//
//

var urlPrefix = "https://file-examples.com/wp-content/uploads/"
var typeForURL = map[string]string{
	// Video
	// Fido identifies 100%
	// constants/mime_types.go misidentifies ogg video as ogg audio
	"2018/04/file_example_AVI_480_750kB.avi": "video/x-msvideo",
	"2018/04/file_example_MOV_480_700kB.mov": "video/quicktime",
	"2017/04/file_example_MP4_480_1_5MG.mp4": "application/mp4",
	"2018/04/file_example_OGG_480_1_7mg.ogg": "video/ogg",
	"2018/04/file_example_WMV_480_1_2MB.wmv": "video/x-ms-wmv",

	// Audio
	// Both Fido and constants/mime_types.go identify 100%
	"2017/11/file_example_MP3_700KB.mp3": "audio/mpeg",
	"2017/11/file_example_WAV_1MG.wav":   "audio/x-wav",
	"2017/11/file_example_OOG_1MG.ogg":   "audio/ogg",

	// Documents
	// Fido fails on: .doc, .docx, .pdf, .ppt, .xls, .xlsx,
	// Fido has 60% failure rate.
	// constants/mime_types.go has 100% success rate.
	"2017/02/file-sample_500kB.doc":       "application/msword",
	"2017/02/file-sample_500kB.docx":      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	"2017/02/file_example_XLS_5000.xls":   "application/vnd.ms-excel",
	"2017/02/file_example_XLSX_5000.xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	"2017/08/file_example_PPT_500kB.ppt":  "application/vnd.ms-powerpoint",
	"2017/10/file-example_PDF_500_kB.pdf": "application/pdf",
	"2017/10/file-sample_500kB.odt":       "application/vnd.oasis.opendocument.text",
	"2017/10/file_example_ODS_5000.ods":   "application/vnd.oasis.opendocument.spreadsheet",
	"2017/10/file_example_ODP_500kB.odp":  "application/vnd.oasis.opendocument.presentation",
	"2019/09/file-sample_500kB.rtf":       "application/rtf",

	// Images
	// Fido fails on .gif, .jpg, .png, .webp: more than 50% failure
	// constants/mime_types.go has 100% success rate.
	"2017/10/file_example_JPG_500kB.jpg":   "image/jpeg",
	"2017/10/file_example_PNG_500kB.png":   "image/png",
	"2017/10/file_example_GIF_500kB.gif":   "image/gif",
	"2017/10/file_example_TIFF_1MB.tiff":   "image/tiff",
	"2017/10/file_example_favicon.ico":     "image/vnd.microsoft.icon",
	"2020/03/file_example_SVG_30kB.svg":    "image/svg+xml",
	"2020/03/file_example_WEBP_250kB.webp": "image/webp",
}

func TestFidoIdentification(t *testing.T) {
	fi := util.NewFormatIdentifier(getScriptPath())
	for urlPath, mimeType := range typeForURL {
		url := urlPrefix + urlPath
		filename := path.Base(urlPath)
		idRecord, err := fi.Identify(url, filename)
		assert.Nil(t, err)
		assert.NotNil(t, idRecord, url)
		require.True(t, idRecord.Succeeded, "FIDO cannot identify "+filename)
		assert.Equal(t, mimeType, idRecord.MimeType, url)
		assert.True(t, (idRecord.MatchType == "signature" || idRecord.MatchType == "extension"), url)
	}
}

func TestStaticIdentification(t *testing.T) {
	for urlPath, mimeType := range typeForURL {
		extension := path.Ext(urlPath)
		assert.Equal(t, mimeType, constants.MimeTypeForExtension[extension])
	}
}
