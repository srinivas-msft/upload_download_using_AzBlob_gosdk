package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

const (
	account = "https://srinivasaccount1.blob.core.windows.net/"
)

func timer(name string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", name, time.Since(start))
	}
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func create_container(client *azblob.Client, containerName string) {
	ctx := context.Background()
	fmt.Printf("Creating a container named %s\n", containerName)
	_, err := client.CreateContainer(ctx, containerName, nil)
	fmt.Println("Container created Successfully!")
	handleError(err)
}

func upload_blob(client *azblob.Client, containerName string, blobName string, filepath string) {
	defer timer("upload_blob")()
	fileHandler, err := os.Open(filepath)
	handleError(err)
	//Upload the file to a block blob
	response, err := client.UploadFile(context.TODO(), containerName, blobName, fileHandler,
		&azblob.UploadFileOptions{
			//BlockSize:   int64(256 * 1024 * 1024),
			//Concurrency: uint16(3),
			// If Progress is non-nil, this function is called periodically as bytes are uploaded.
			Progress: func(bytesTransferred int64) {
				//fmt.Println(bytesTransferred)
			},
		})

	fmt.Println("blob upload Successfully!")
	return

	fmt.Printf("ETAG value for upload file is %s\n", *response.ETag)

	svc := client.ServiceClient()
	bb := svc.NewContainerClient(containerName).NewBlockBlobClient(blobName)
	bb_properties, err := bb.GetProperties(context.Background(), &blob.GetPropertiesOptions{})
	handleError(err)
	fmt.Println(bb_properties)
	fmt.Printf("committed Block count : %d, Blob Sequence Number : %d\n", bb_properties.BlobCommittedBlockCount, bb_properties.BlobSequenceNumber)

	res1, err := bb.GetBlockList(context.Background(), blockblob.BlockListTypeAll, &blockblob.GetBlockListOptions{})

	if err != nil {
		fmt.Println(err)
		return
	}
	Blocklist := res1.BlockList
	fmt.Println("Commited blocks")
	//for _, v := range Blocklist.CommittedBlocks {
	//fmt.Println(v.Name)
	//}
	fmt.Println("Uncommitted blocks")
	for _, v := range Blocklist.UncommittedBlocks {
		fmt.Println(v.Name)
	}
	fileHandler.Close()

}

func get_blocklist(client *azblob.Client, containerName string, blobName string) {

	svc := client.ServiceClient()
	bb := svc.NewContainerClient(containerName).NewBlockBlobClient(blobName)
	bb_properties, err := bb.GetProperties(context.Background(), &blob.GetPropertiesOptions{})
	handleError(err)
	fmt.Println(bb_properties)
	fmt.Printf("committed Block count : %d, Blob Sequence Number : %d\n", bb_properties.BlobCommittedBlockCount, bb_properties.BlobSequenceNumber)

	res1, err := bb.GetBlockList(context.Background(), blockblob.BlockListTypeAll, &blockblob.GetBlockListOptions{})

	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("BlockList: %s %d %d\n", bb.URL(), len(res1.BlockList.CommittedBlocks), len(res1.BlockList.UncommittedBlocks))
	Blocklist := res1.BlockList
	fmt.Println("Commited blocks")
	for _, v := range Blocklist.CommittedBlocks {
		fmt.Println(v.Name)
	}
	fmt.Println("Uncommitted blocks")
	for _, v := range Blocklist.UncommittedBlocks {
		fmt.Println(v.Name)
	}
}

func upload_uncommitted_blocks(client *azblob.Client, containerName string, blobName string, blockids []string) {
	svc := client.ServiceClient()
	bb := svc.NewContainerClient(containerName).NewBlockBlobClient(blobName)
	res, err := bb.CommitBlockList(context.Background(), blockids, &blockblob.CommitBlockListOptions{})
	fmt.Println(res)
	handleError(err)

}

func upload_append_blob(client *azblob.Client, containerName string, blobName string, filename string) {
	defer timer("upload_append_blob")()
	fileHandler, err := os.Open(filename)
	handleError(err)
	stat, err := fileHandler.Stat()
	filesize := stat.Size()
	var blocksize int64 = 4 * 1024 * 1024
	n := (filesize + blocksize - 1) / blocksize
	fmt.Printf("file size is %d\n", filesize)
	buf := make([]byte, 4*1024*1024)
	svc := client.ServiceClient()
	bb := svc.NewContainerClient(containerName).NewAppendBlobClient(blobName)
	var i int64 = 1
	for ; i <= n; i++ {
		_, err = bufio.NewReader(fileHandler).Read(buf)
		handleError(err)
		_, err = bb.AppendBlock(context.Background(), streaming.NopCloser(bytes.NewReader(buf)), nil)
		handleError(err)
	}
	fileHandler.Close()

}

func download_blob(client *azblob.Client, containerName string, blobName string, filename string) {
	defer timer("Download blob")()
	fileHandler, err := os.Create(filename)
	handleError(err)
	defer fileHandler.Close()
	//destFile, err := os.OpenFile("temp-photo.png", os.O_RDWR|os.O_CREATE, 0755)

	// nBytes, err := client.DownloadFile(context.Background(), containerName, blobName, fileHandler,
	// 	&azblob.DownloadFileOptions{})
	// fmt.Println(nBytes)
	svc := client.ServiceClient()
	bb := svc.NewContainerClient(containerName).NewBlockBlobClient(blobName)
	bytes, err := bb.DownloadFile(context.Background(), fileHandler, &blob.DownloadFileOptions{})
	fmt.Println(bytes)

	// Assert download was successful
	handleError(err)
	fmt.Println("blob download Successfully!")
}
func remove_blob(client *azblob.Client, containerName string, blobName string) {
	_, err := client.DeleteBlob(context.TODO(), containerName, blobName, nil)
	handleError(err)
}

func main() {
	argv := os.Args
	if len(argv) < 2 {
		panic("Enter valid command line arguments")
	}
	mode := argv[1][0]
	accountName, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_NAME")
	if !ok {
		panic("AZURE_STORAGE_ACCOUNT_NAME could not be found")
	}

	accountKey, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_KEY")
	if !ok {
		panic("AZURE_STORAGE_ACCOUNT_KEY could not be found")
	}

	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	handleError(err)

	client, err := azblob.NewClientWithSharedKeyCredential(fmt.Sprintf("https://%s.blob.core.windows.net/", accountName), cred, nil)
	if err != nil {
		handleError(err)
	}
	fmt.Println("Successfully created client")
	switch mode {
	case 'c': //create container(argc: containername)
		create_container(client, argv[2])

	case 'u': //upload blob(argc: containername, blobname, filename)
		upload_blob(client, argv[2], argv[3], argv[4])
		//upload_append_blob(client, argv[2], argv[3], argv[4])
		//upload_uncommitted_blocks(client, arg[2], arg[3], arg[4])
		//blockids := []string{argv[4], argv[5]}
		//upload_uncommitted_blocks(client, argv[2], argv[3], blockids)

	case 'd': //Download blob(argc: containername, blobname, filename)
		download_blob(client, argv[2], argv[3], argv[4])
	case 'r': //Remove blob
		remove_blob(client, argv[2], argv[3])

	default:
		panic("Enter valid command line arguments")

	}

}
