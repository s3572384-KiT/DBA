package model;

import java.util.Objects;

/**
 * Sensor Entity
 * important - code reference:
 * https://www.tutorialspoint.com/java/java_documentation.htm
 *
 * @author Kit T
 * @version 1.0
 * @since 11-April-2021
 */
public class Sensor {
    private final int id;
    private final String name;

    public Sensor(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Sensor sensor = (Sensor) o;
        return id == sensor.id && Objects.equals(name, sensor.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return String.format("Sensor: %d-%s", id, name);
    }
}