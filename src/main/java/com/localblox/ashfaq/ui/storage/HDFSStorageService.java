package com.localblox.ashfaq.ui.storage;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * HDFS storage service implementation.
 */
@Service
public class HDFSStorageService implements StorageService {

    private static final Logger log = LoggerFactory.getLogger(HDFSStorageService.class);

    private static final String CONTENT_TEXT_CSV = "text/csv";

    private static final String PATH_TMP_SUFFIX = ".tmp";

    private static final String PATH_CSV_SUFFIX = ".csv";

    private String uploadDir;

    public HDFSStorageService(StorageProperties properties) {
        uploadDir = properties.getLocation();
    }

    @Override
    public void init() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void store(MultipartFile file) {

        if (!CONTENT_TEXT_CSV.equals(file.getContentType())) {
            throw new IllegalStateException("Content type must be text/csv instead: " + file.getContentType());
        }

        StopWatch watch = new StopWatch();
        watch.start();

        Path filenamePath = new Path(uploadDir + "/" + UUID.randomUUID());

        log.info("start saving file to: {}, size = {} bytes", filenamePath, file.getSize());

        Configuration entries = new Configuration();
        entries.set("fs.defaultFS", uploadDir);

        try (FileSystem fs = FileSystem.get(entries);
             FSDataOutputStream fsous = fs.create(filenamePath.suffix(PATH_TMP_SUFFIX))) {

            fsous.write(file.getBytes());

            Path result = renameFile(fs, filenamePath);

            log.info("file {} stored, time = {} ms", result, watch.getTime());

        } catch (IOException ioe) {
            log.error("IOException during operation with file [{}] : {}", filenamePath, ioe.toString(), ioe);
        } catch (Exception e) {
            log.error("Error occurred while loading file [{}] to storage", filenamePath, e);
        }
    }

    /**
     * Rename file upon writing completion from '<UUID>.tmp' to '<UUID>.csv'
     *
     * @param fs           - File system
     * @param filenamePath - File path
     */
    private Path renameFile(final FileSystem fs, final Path filenamePath) throws IOException {

        Path from = filenamePath.suffix(PATH_TMP_SUFFIX);
        Path to = filenamePath.suffix(PATH_CSV_SUFFIX);

        if (!(fs.rename(from, to))) {
            log.warn("can not rename file {} to {}", from, to);
            return from;
        }

        return to;
    }

    @Override
    public Stream<Path> loadAll() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Path load(String filename) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Resource loadAsResource(String filename) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void deleteAll() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
