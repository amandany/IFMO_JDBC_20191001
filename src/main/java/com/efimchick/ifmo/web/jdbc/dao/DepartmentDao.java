package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {
    private Employee newEmployee(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String firstName = resultSet.getString("firstName");
        String lastName = resultSet.getString("lastName");
        String middleName = resultSet.getString("middleName");
        String position = resultSet.getString("position");
        String hireDate = resultSet.getString("hireDate");
        String salary = resultSet.getString("salary");
        int managerId = resultSet.getInt("manager");
        int departmentId = resultSet.getInt("department");

        return new Employee(
                new BigInteger(id),
                new FullName(firstName, lastName, middleName),
                Position.valueOf(position),
                LocalDate.parse(hireDate),
                new BigDecimal(salary),
                BigInteger.valueOf(managerId),
                BigInteger.valueOf(departmentId));
    }

    private ResultSet executeRequest(String request) throws SQLException {
        try {
            return ConnectionSource.instance().createConnection().createStatement().executeQuery(request);
        }
        catch (SQLException e){
            return null;
        }
    }

    private Department newDepartment(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String name = resultSet.getString("name");
        String location = resultSet.getString("location");
        return new Department(new BigInteger(id), name, location);
    }


    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                try {
                    ResultSet resultSet = executeRequest(
                            "SELECT * FROM employee WHERE department = "
                                    + department.getId());

                    List<Employee> employees = new ArrayList<>();
                    while (resultSet.next()) {
                        employees.add(newEmployee(resultSet));
                    }

                    return employees;
                }
                catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                try {
                    ResultSet resultSet = executeRequest(
                            "SELECT * FROM employee WHERE manager = "
                                    + employee.getId());

                    List<Employee> employees = new ArrayList<>();
                    while (resultSet.next()) {
                        employees.add(newEmployee(resultSet));
                    }

                    return employees;
                }
                catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                try {
                    ResultSet resultSet = executeRequest(
                            "SELECT * FROM employee WHERE id = "
                                    + Id.toString());
                    if (resultSet.next())
                        return Optional.of(newEmployee(resultSet));
                    else
                        return Optional.empty();
                }
                catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Employee> getAll() {
                try {
                    ResultSet resultSet = executeRequest(
                            "SELECT * FROM employee");
                    List<Employee> employees = new ArrayList<>();
                    while (resultSet.next()) {
                        employees.add(newEmployee(resultSet));
                    }
                    return employees;
                }
                catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Employee save(Employee employee) {
                try {
                    executeRequest(
                            "INSERT INTO employee VALUES ('"
                                    + employee.getId()                       + "', '"
                                    + employee.getFullName().getFirstName()  + "', '"
                                    + employee.getFullName().getLastName()   + "', '"
                                    + employee.getFullName().getMiddleName() + "', '"
                                    + employee.getPosition()                 + "', '"
                                    + employee.getManagerId()                + "', '"
                                    + Date.valueOf(employee.getHired())      + "', '"
                                    + employee.getSalary()                   + "', '"
                                    + employee.getDepartmentId()             + "')"
                    );
                    return employee;
                }
                catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public void delete(Employee employee) {
                try {
                    ConnectionSource.instance().createConnection().createStatement().execute(
                            "DELETE FROM employee WHERE ID = " + employee.getId().toString());
                }
                catch (SQLException e) {}
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                try {
                    ResultSet resultSet = executeRequest(
                            "SELECT * FROM department WHERE id = "
                                    + Id.toString());

                    if (resultSet.next())
                        return Optional.of(newDepartment(resultSet));
                    else
                        return Optional.empty();
                }
                catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public Department save(Department department) {
                try {
                    if (getById(department.getId()).equals(Optional.empty())) {
                        executeRequest(
                                "INSERT INTO department VALUES ('" +
                                        department.getId()       + "', '" +
                                        department.getName()     + "', '" +
                                        department.getLocation() + "')"
                        );
                    } else {
                        executeRequest(
                                "UPDATE department SET " +
                                        "NAME = '"     + department.getName()     + "', " +
                                        "LOCATION = '" + department.getLocation() + "' " +
                                        "WHERE ID = '" + department.getId()       + "'"
                        );
                    }
                    return department;
                }
                catch (SQLException e) {
                    return null;
                }
            }
            @Override
            public List<Department> getAll() {
                try {
                    ResultSet resultSet = executeRequest("SELECT * FROM department");

                    List<Department> deps = new ArrayList<>();
                    while (resultSet.next()) {
                        deps.add(newDepartment(resultSet));
                    }
                    return deps;
                }
                catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public void delete(Department department) {
                try {
                    ConnectionSource.instance().createConnection().createStatement().execute(
                            "DELETE FROM department WHERE ID = " + department.getId().toString());
                }
                catch (SQLException e) {}
            }
        };
    }
}