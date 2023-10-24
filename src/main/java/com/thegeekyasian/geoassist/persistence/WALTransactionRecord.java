package com.thegeekyasian.geoassist.persistence;

import com.github.f4b6a3.ulid.Ulid;
import com.github.f4b6a3.ulid.UlidCreator;
import com.thegeekyasian.geoassist.kdtree.KDTreeObject;

import java.io.Serializable;
import java.util.Optional;

/**
 * Represents a record of a single transaction operation on a KDTree.
 * Each record contains details about the operation to be performed,
 * which could be one of INSERT, DELETE, or UPDATE.
 * <p>
 * Depending on the operation type, different data members are utilized.
 * The class handles these variations by allowing nullable fields and
 * providing clearer access patterns to these potentially null values.
 *
 * @param <T> the type of the keys maintained by the KDTree, indicating the object's identifier.
 * @param <O> the type of the values or data associated with the keys.
 * @author 1919kiran (<a href="https://github.com/1919kiran">...</a>)
 */
public class WALTransactionRecord<T, O> implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Ulid transactionId;
    private final WALOperation operation;
    private final KDTreeObject<T, O> kdTreeObject;
    private final T id;
    private final O data;

    private WALTransactionRecord(WALOperation operation, KDTreeObject<T, O> kdTreeObject, T id, O data) {
        this.transactionId = UlidCreator.getMonotonicUlid();
        this.operation = operation;
        this.kdTreeObject = kdTreeObject; // Will be null for update and delete operations.
        this.id = id; // Will be null for insert operation, as kdTreeObject is directly used.
        this.data = data; // Will be null for insert and delete operations, as only id is needed.
    }

    // Static factory methods for creating instances
    public static <T, O> WALTransactionRecord<T, O> forInsert(KDTreeObject<T, O> kdTreeObject) {
        return new WALTransactionRecord<>(WALOperation.INSERT, kdTreeObject, null, null);
    }

    public static <T, O> WALTransactionRecord<T, O> forUpdate(T id, O data) {
        return new WALTransactionRecord<>(WALOperation.UPDATE, null, id, data);
    }

    public static <T, O> WALTransactionRecord<T, O> forDelete(T id) {
        return new WALTransactionRecord<>(WALOperation.DELETE, null, id, null);
    }

    public WALOperation getOperation() {
        return operation;
    }

    public Optional<KDTreeObject<T, O>> getKdTreeObject() {
        return Optional.ofNullable(kdTreeObject);
    }

    public Optional<T> getId() {
        return Optional.ofNullable(id);
    }

    public Optional<O> getData() {
        return Optional.ofNullable(data);
    }

    @Override
    public String toString() {
        return "WALTransactionRecord{" + "transactionId=" + transactionId + ", operation=" + operation + ", kdTreeObject=" + kdTreeObject + ", id=" + id + ", data=" + data + '}';
    }

    public enum WALOperation {
        INSERT, DELETE, UPDATE
    }
}
