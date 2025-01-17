package com.thegeekyasian.geoassist.kdtree;

import com.thegeekyasian.geoassist.kdtree.geometry.Point;

import java.io.Serializable;

/**
 * The KDTreeObject class represents an object that can be inserted into a KD-Tree.
 * Each object is associated with a point in the k-dimensional space and has
 * an identifier of type T and a data object of type O.
 *
 * @param <T>
 *     describes the identifier of the K-d Tree Object,
 *     that is being inserted in the tree. For example ID or UUID of the Object.
 *
 * @param <O>
 *     describes the object that is inserted in the tree.
 *     For example Vendor, Restaurant, Franchise, etc.
 *
 * @author The Geeky Asian
 */
public final class KDTreeObject<T, O> implements Serializable {

	private static final long serialVersionUID = 1L;

	private T id;

	private O data;

	private Point point;

	private KDTreeObject(Builder<T, O> builder, Point point) {
		this.id = builder.id;
		this.data = builder.data;
		this.point = point;
	}

	public T getId() {
		return this.id;
	}

	public void setId(T id) {
		this.id = id;
	}

	public O getData() {
		return this.data;
	}

	public void setData(O data) {
		this.data = data;
	}

	public Point getPoint() {
		return this.point;
	}

	public void setPoint(Point point) {
		this.point = point;
	}

	public static class Builder<T, O> {
		private T id;

		private O data;

		private double latitude;

		private double longitude;

		public Builder<T, O> id(T id) {
			this.id = id;
			return this;
		}

		public Builder<T, O> data(O data) {
			this.data = data;
			return this;
		}

		public Builder<T, O> latitude(double latitude) {
			this.latitude = latitude;
			return this;
		}

		public Builder<T, O> longitude(double longitude) {
			this.longitude = longitude;
			return this;
		}

		public KDTreeObject<T, O> build() {
			Point point = new Point.Builder()
					.latitude(this.latitude)
					.longitude(this.longitude)
					.build();
			return new KDTreeObject<>(this, point);
		}
	}
}
