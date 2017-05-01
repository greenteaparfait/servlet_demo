/*
   For step-by-step instructions on connecting your Android application to this backend module,
   see "App Engine Java Servlet Module" template documentation at
   https://github.com/GoogleCloudPlatform/gradle-appengine-templates/tree/master/HelloWorld
*/
package com.example.Onejae.myapplication.backend;

import com.google.appengine.repackaged.com.google.api.client.http.HttpTransport;
import com.google.appengine.repackaged.com.google.api.client.http.javanet.NetHttpTransport;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.ListItem;
import com.google.appengine.tools.cloudstorage.ListOptions;
import com.google.appengine.tools.cloudstorage.ListResult;
import com.google.appengine.tools.cloudstorage.RetryParams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * A simple servlet that proxies reads and writes to its Google Cloud Storage bucket.
 */
@SuppressWarnings("serial")
public class MyServlet extends HttpServlet {

    /** HTTP status code for a resource that wasn't found. */
    private static final int HTTP_NOT_FOUND = 404;
    /** HTTP status code for a resource that was found. */
    private static final int HTTP_OK = 200;
    /** The base endpoint for Google Cloud Storage api calls. */
    private static final String GCS_URI =  "http://storage.googleapis.com";
    /** Global configuration of Google Cloud Storage OAuth 2.0 scope. */
    private static final String STORAGE_SCOPE = "https://www.googleapis.com/auth/devstorage.full_control";
    /** Global instance of the HTTP transport. */
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    /** Global instance of HTML reference to XSL style sheet. */
    private static final String XSL = "\n<?xml-stylesheet href=\"/xsl/listing.xsl\" type=\"text/xsl\"?>\n";
    private static final String DEFAULT_BUCKET = "testcloudbucket";

    /**
     * This is where backoff parameters are configured. Here it is aggressively retrying with
     * backoff, up to 10 times but taking no more that 15 seconds total to do so.
     */
    private final GcsService gcsService = GcsServiceFactory.createGcsService(new RetryParams.Builder()
            .initialRetryDelayMillis(10)
            .retryMaxAttempts(10)
            .totalRetryPeriodMillis(15000)
            .build());

    /**Used below to determine the size of chucks to read in. Should be > 1kb and < 10MB */
    private static final int BUFFER_SIZE = 2 * 1024 * 1024;


    //[START doGet]
    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        resp.setContentType("text/plain");
        ServletOutputStream out = resp.getOutputStream();
        String temp = "";
        GcsFilename fileName = getFileName(req);
/*
        temp = "Check point 1 \n";
        out.print(temp);
        out.flush();
*/
     // Testing bucket and object values
        out.print("***");
        out.print(fileName.getBucketName());
        out.flush();
        out.print("***");
        out.print(fileName.getObjectName());
        out.flush();
        out.print("***");
        out.flush();

        out.print("***\n");
        out.flush();


        switch (fileName.getBucketName()) {

            case "testcloudbucket":

                switch (fileName.getObjectName()) {
                    case "":  // Listing objects in the bucket "testcloudbucket"
                        try {
                            GcsService gcsService = GcsServiceFactory.createGcsService(RetryParams.getDefaultInstance());
                            // AppIdentityService appIdentity = AppIdentityServiceFactory.getAppIdentityService();
                            ListResult result = gcsService.list(DEFAULT_BUCKET, ListOptions.DEFAULT);
                            while (result.hasNext()){
                                ListItem l = result.next();
                                String name = l.getName();

                                out.println("Name: " + name);
                            }
                            out.flush();
                            /*  // For debugging
                            temp = "result: no object exists\n";
                            out.print(temp);
                            out.flush();
                            resp.setStatus(HTTP_OK); */
                        }catch (Throwable e) {
                            resp.sendError(HTTP_NOT_FOUND, e.getMessage()); }
                        break;
                    default:  // Download content of an object in the bucket "testcloudbucket"
                        GcsInputChannel readChannel = gcsService.openPrefetchingReadChannel(fileName, 0, BUFFER_SIZE);
                        copy(Channels.newInputStream(readChannel), resp.getOutputStream());
                            /* // For debugging
                            temp = "result: object exists\n";
                            out.print(temp);
                            out.flush();
                            resp.setStatus(HTTP_OK);
                            */
                        break;
                }
                break;

            default:
                temp = "The bucket name is not testcloudbucket";
                out.print(temp);
                out.flush();
                resp.setStatus(HTTP_OK);
                break;
        }

    }

//[END doGet]

    /**
     * Writes the payload of the incoming post as the contents of a file to GCS.
     * If the request path is /gcs/Foo/Bar this will be interpreted as
     * a request to create a GCS file named Bar in bucket Foo.
     */
//[START doPost]
    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        try {
            GcsFileOptions instance = GcsFileOptions.getDefaultInstance();
            GcsFilename fileName = getFileName(req);
            GcsOutputChannel outputChannel;
            outputChannel = gcsService.createOrReplace(fileName, instance);
            copy(req.getInputStream(), Channels.newOutputStream(outputChannel));
        }catch (Throwable e) {
            resp.sendError(HTTP_NOT_FOUND, e.getMessage()); }
    }
//[END doPost]

    private GcsFilename getFileName(HttpServletRequest req) {
        String[] splits = req.getRequestURI().split("/", 4);
        if (!splits[0].equals("") || !splits[1].equals("gcs")) {
            throw new IllegalArgumentException("The URL is not formed as expected. " +
                    "Expecting /gcs/<bucket>/<object>");
        }
        return new GcsFilename(splits[2], splits[3]);
    }

    /**
     * Transfer the data from the inputStream to the outputStream. Then close both streams.
     */
    private void copy(InputStream input, OutputStream output) throws IOException {
        try {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead = input.read(buffer);
            while (bytesRead != -1) {
                output.write(buffer, 0, bytesRead);
                bytesRead = input.read(buffer);
            }
        } finally {
            input.close();
            output.close();
        }
    }
}
