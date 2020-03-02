package com.efimchick.ifmo.web.jdbc;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
        return result -> {
            Set<Employee> employeeSet = new HashSet<>();
            try {
                while (result.next()) {
                    employeeSet.add(getEmployee(result));
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            return employeeSet;
        };
    }

    private Employee getEmployee(ResultSet result) throws SQLException {
        try {
            return new Employee(
                    new BigInteger(result.getString("ID")),
                    new FullName(
                            result.getString("FIRSTNAME"),
                            result.getString("LASTNAME"),
                            result.getString("MIDDLENAME")),
                    Position.valueOf(result.getString("POSITION")),
                    LocalDate.parse(result.getString("HIREDATE")),
                    result.getBigDecimal("SALARY"),
                    getManager(result, result.getInt("MANAGER"))
            );
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public Employee getManager(ResultSet result, int managerId) throws SQLException {
        Employee manager = null;
        int rowNumber = result.getRow();
        result.beforeFirst();

        while (result.next()) {
            if (result.getInt("ID") == managerId) {
                manager = getEmployee(result);
                break;
            }
        }
        result.absolute(rowNumber);
        return manager;
    }
}
