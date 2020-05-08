package app.dao.query;

public class Coordinate {
    public String referenceName;
    public Long startPosition;
    public Long endPosition;

    public Coordinate() {}

    public Coordinate(String referenceName, Long startPosition, Long endPosition) {
        this.referenceName = referenceName;
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }
}
