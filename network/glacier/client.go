package glacier

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/minio/minio-go/v6/pkg/signer"
)

const DaysToLiveInRestoreBucket = 10
const DefaultTier = "Standard"

// Restore sends a restoration request to Glacier, asking for the item at url
// to be copied to S3. Since restoration typically takes several hours,
// you should call this periodically and check the response code.
//
// Status 200 (OK) means the item has already been copied to S3.
//
// Status 202 (Accepted) means Glacier accepted the request to copy the
// item to S3, but it's not been copied yet, so check back later.
//
// Status 409 (Conflict) means the restoration is in progress but has
// not yet completed. Check again later to see if it's done.
//
// Status 503 (Service Not Available) means Glacier expedited retrievals
// are currently not available.
//
// For more info, see
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html
func Restore(context *common.Context, url string) (int, error) {
	context.Logger.Infof("Requesting restoration of %s", url)
	postURL := fmt.Sprintf("%s?restore=", url)
	body := getRequestBody()
	request, err := http.NewRequest(http.MethodPost, postURL, strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Length", strconv.Itoa(len(body)))

	// Note that for now, we only use AWS Glacier. There is no other Glacier
	// provider, so constants.StorageProviderAWS will do us for now.
	creds := context.Config.S3Credentials[constants.StorageProviderAWS]
	if creds == nil {
		return 0, fmt.Errorf("Can't find credentials for %s", constants.StorageProviderAWS)
	}
	signedRequest := signer.SignV4(*request, creds.KeyID, creds.SecretKey, "", url)

	sha := sha256.New()
	io.Copy(sha, strings.NewReader(body))
	signedRequest.Header.Set("X-Amz-Content-Sha256", fmt.Sprintf("%x", sha.Sum(nil)))
	// signedRequest.Header.Set("Host", "s3.amazonaws.com")

	// --- DEBUG ---
	for k, v := range signedRequest.Header {
		context.Logger.Infof("Request Header: %s = %s", k, v)
	}
	context.Logger.Infof("Request body: %s", body)
	// --- DEBUG ---

	httpClient := &http.Client{}
	response, err := httpClient.Do(signedRequest)
	if err != nil {
		context.Logger.Errorf("Glacier restore request returned error %v", err)
		return 0, err
	}
	defer response.Body.Close()

	// These responses are short snippets of XML
	buf := new(strings.Builder)
	_, err = io.Copy(buf, response.Body)
	if err == nil {
		context.Logger.Infof("Glacier restore %s returned code %d, body %s", url, response.StatusCode, buf.String())
	} else {
		context.Logger.Warningf("Glacier restore %s: could not read response", url)
	}

	return response.StatusCode, nil
}

func getRequestBody() string {
	str := "<RestoreRequest><Days>%d</Days><GlacierJobParameters><Tier>%s</Tier></GlacierJobParameters></RestoreRequest>"
	return fmt.Sprintf(str, DaysToLiveInRestoreBucket, DefaultTier)
}
