package main

// required env GO111MODULE=on
import (
	"archive/tar"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/minio/minio-go/v6"
	"io"
	"os"
	"path"
)

const srcBucket = "aptrust.poc.receiving"
const unpackBucket = "aptrust.poc.unpacked"
const presBucket = "aptrust.poc.preservation"
const testFile = "poc-test-01.tar"
const prefix = "0001"

var s3Client *minio.Client

func main() {
	initS3Client()
	readTarFile()
	copyTarFileContents()
	copyToPreservation()
	return
}

func copyTarFileContents() {
	s3Stream := getS3FileStream()
	defer s3Stream.Close()
	tarReader := tar.NewReader(s3Stream)
	for {
		if copyNextToStaging(tarReader) == false {
			break
		}
	}
}

func copyNextToStaging(tarReader *tar.Reader) bool {
	header, err := tarReader.Next()
	if err == io.EOF {
		fmt.Println("End of tar file")
		return false
	}
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
		fmt.Println("Skipping non-file entry", header.Name)
	} else {
		fmt.Println(header.Name, header.Size)
		copyToStaging(tarReader, header)
	}
	return true
}

func copyToStaging(tarReader *tar.Reader, header *tar.Header) {
	// Prefix will be WorkItem id
	objName := fmt.Sprintf("%s/%s", prefix, header.Name)
	bytes, err := s3Client.PutObject(unpackBucket, objName, tarReader, header.Size, minio.PutObjectOptions{})
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Copied", bytes, "to staging for", objName)
	}
}

func readTarFile() {
	s3Stream := getS3FileStream()
	defer s3Stream.Close()
	tarReader := tar.NewReader(s3Stream)
	for {
		if readNextEntry(tarReader) == false {
			break
		}
	}
}

func readNextEntry(tarReader *tar.Reader) bool {
	header, err := tarReader.Next()
	if err == io.EOF {
		fmt.Println("End of tar file")
		return false
	}
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
		fmt.Println("Skipping non-file entry", header.Name)
	} else {
		fmt.Println(header.Name, header.Size)
		// Here, we would record checksums, file path, size, etc.
		printChecksums(tarReader)
	}
	return true
}

func printChecksums(tarReader *tar.Reader) {
	md5Hash := md5.New()
	sha256Hash := sha256.New()
	multiWriter := io.MultiWriter(md5Hash, sha256Hash)
	io.Copy(multiWriter, tarReader)
	fmt.Println("    md5:", fmt.Sprintf("%x", md5Hash.Sum(nil)))
	fmt.Println("    sha256:", fmt.Sprintf("%x", sha256Hash.Sum(nil)))
}

func copyToPreservation() {
	count := 0
	doneCh := make(chan struct{})
	defer close(doneCh)
	objectCh := s3Client.ListObjectsV2(unpackBucket, prefix, true, doneCh)
	for object := range objectCh {
		count += 1
		if object.Err != nil {
			fmt.Println(object.Err)
			return
		}
		//fmt.Println(object)
		newName := fmt.Sprintf("%04d-%s", count, path.Base(object.Key))
		//fmt.Println(newName)

		src := minio.NewSourceInfo(unpackBucket, object.Key, nil)
		dst, err := minio.NewDestinationInfo(presBucket, newName, nil,
			map[string]string{
				"PathInBag": object.Key,
			})
		if err != nil {
			fmt.Println("Error creating dest object:", err.Error())
			continue
		}
		err = s3Client.CopyObject(dst, src)
		if err != nil {
			fmt.Println("Error copying object to preservation:", err.Error())
		} else {
			fmt.Println("Copied", object.Key, "to", newName)
		}
	}
}

func getS3FileStream() *minio.Object {
	obj, err := s3Client.GetObject(srcBucket, testFile, minio.GetObjectOptions{})
	if err != nil {
		panic(err)
	}
	return obj
}

func initS3Client() {
	var err error
	s3Client, err = minio.New("s3.amazonaws.com",
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		true)
	if err != nil {
		panic(err)
	}
}
