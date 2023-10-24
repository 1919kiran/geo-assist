package com.thegeekyasian.geoassist.persistence;

import com.github.f4b6a3.ulid.UlidCreator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.thegeekyasian.geoassist.core.GeoAssistException;
import com.thegeekyasian.geoassist.kdtree.KDTree;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;


/**
 * WALManager is responsible for managing the Write-Ahead Logging (WAL) mechanism.
 * This class provides functionalities to append transaction records to the log and
 * to replay logs to recover the state of a KDTree instance.
 * <p>
 *
 * @param <T> the type of keys maintained by the KDTree, representing the multidimensional points.
 * @param <O> the type of the values or payloads that the points represented by {@code T} are associated with.
 * @author 1919kiran (<a href="https://github.com/1919kiran">...</a>)
 * @see KDTree
 * @see WALTransactionRecord
 */
public class WALManager<T, O> {

    private static final String FILE_FORMAT = ".wal";
    private static final OpenOption[] FILE_WRITE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND};
    private final Path logDirectoryPath;
    private final Path walFile;
    private final Gson gson;
    private final Type recordType;

    /**
     * Initializes a new Write-Ahead Logging (WAL) manager for managing transaction logs.
     * Creates a new WAL log file with a unique name generated using Monotonic ULID and file format.
     *
     * @param logDirectoryPath The path to the directory where WAL logs will be stored.
     */
    public WALManager(String logDirectoryPath) {
        this.logDirectoryPath = Paths.get(logDirectoryPath);
        this.walFile = Paths.get(logDirectoryPath, UlidCreator.getMonotonicUlid().toString().toLowerCase().concat(FILE_FORMAT));
        this.gson = new GsonBuilder().create();
        this.recordType = new TypeToken<WALTransactionRecord<T, O>>() {
        }.getType();
    }

    /**
     * Appends a transaction record to the log file. The records are stored in a JSON format,
     * and each record is written as a new line in the log file. This method ensures that
     * the write operation is appended to the file to prevent overwriting of existing logs.
     *
     * @param record the WALTransactionRecord object representing the operation to be logged.
     */
    public void appendToLog(WALTransactionRecord<T, O> record) {
        ensureDirectoryExists();
        try (BufferedWriter writer = Files.newBufferedWriter(this.walFile, StandardCharsets.UTF_8, FILE_WRITE_OPTIONS)) {
            writer.write(gson.toJson(record));
            writer.newLine();
        } catch (IOException e) {
            throw new GeoAssistException("Failed to write record", e);
        }
    }

    /**
     * Replays the logs from the log file and applies the transactions to the provided {@code KDTree} instance.
     * This method is used typically during the recovery process where the KDTree is being reconstructed
     * to its last consistent state before a crash.
     * <p>
     * Each line from the log file represents a single transaction record, which is deserialized and
     * then applied to the {@code KDTree}.
     *
     * @param kdTree  the {@code KDTree} instance on which the logged operations are to be replayed.
     * @param walFile a {@code String} specifying the absolute path of the WAL file to replay.
     */
    public void replayLogs(KDTree<T, O> kdTree, String walFile) {
        Path walPath = getValidatedWALFilePath(walFile);
        try (BufferedReader reader = Files.newBufferedReader(walPath, StandardCharsets.UTF_8)) {
            reader.lines().forEach(line -> {
                WALTransactionRecord<T, O> record = gson.fromJson(line, recordType);
                applyToKDTree(kdTree, record);
            });
        } catch (IOException e) {
            throw new GeoAssistException("Failed to read records", e);
        }
    }

    /**
     * Replays the logs from the log file and applies the transactions to the provided {@code KDTree} instance.
     * This method is used typically during the recovery process where the {@code KDTree} is being reconstructed
     * to its last consistent state before a crash.
     * <p>
     * Each line from the log file represents a single transaction record, which is deserialized and
     * then applied to the {@code KDTree}.
     *
     * @param kdTree the {@code KDTree} instance on which the logged operations are to be replayed.
     */
    public void replayLogs(KDTree<T, O> kdTree) throws GeoAssistException {
        File latestFile = retrieveLatestWALFile();
        if (latestFile == null) {
            throw new GeoAssistException("No log files to replay in directory: " + this.logDirectoryPath.toString());
        }
        replayLogs(kdTree, latestFile.getAbsolutePath());
    }

    private File retrieveLatestWALFile() {
        File dir = new File(this.logDirectoryPath.toUri());
        Optional<File> latestFile = Optional.ofNullable(dir.listFiles((d, name) -> name.endsWith(FILE_FORMAT))).flatMap(files -> Arrays.stream(files)
                // the latest file has the max value as filenames are based on ULID.
                .max(Comparator.comparing(File::getName)));
        return latestFile.orElse(null);
    }

    private Path getValidatedWALFilePath(String walFile) throws GeoAssistException {
        Path walPath = Paths.get(walFile);
        if (!Files.exists(walPath)) {
            walPath = this.logDirectoryPath.resolve(walFile); // prepend the directory path
            if (!Files.exists(walPath)) {
                throw new GeoAssistException("WAL file does not exist at the specified location: " + walFile);
            }
        }
        return walPath;
    }

    private void ensureDirectoryExists() {
        // Assuming walFile is your file's Path object
        Path directoryPath = this.walFile.getParent();
        // Check if the directory exists, if not, create it
        if (directoryPath != null && Files.notExists(directoryPath)) {
            try {
                Files.createDirectories(directoryPath);
            } catch (IOException e) {
                throw new RuntimeException("Could not create directory structure: " + directoryPath, e);
            }
        }
    }

    /**
     * This is used to apply a single deserialized transaction to the {@code KDTree}.
     * Depending on the operation specified in the record (INSERT, DELETE, UPDATE),
     * the corresponding action will be taken on the {@code KDTree}.
     *
     * @param kdTree the KDTree instance to which the transaction is to be applied.
     * @param record the WALTransactionRecord object containing details of the operation.
     */
    private void applyToKDTree(KDTree<T, O> kdTree, WALTransactionRecord<T, O> record) {
        switch (record.getOperation()) {
            case INSERT:
                record.getKdTreeObject().ifPresent(kdTree::insert);
                break;
            case DELETE:
                record.getId().ifPresent(kdTree::delete);
                break;
            case UPDATE:
                // For update, both key and data are required. So, we ensure both are present.
                if (record.getId().isPresent() && record.getData().isPresent()) {
                    T key = record.getId().get();
                    O data = record.getData().get();
                    kdTree.update(key, data);
                } else {
                    throw new GeoAssistException("Incomplete information for update in record: " + record);
                }
                break;
            default:
                throw new GeoAssistException("Unknown operation in transaction record");
        }
    }

}
