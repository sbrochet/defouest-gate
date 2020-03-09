package main;

import java.sql.*;
import main.IniFile;

public class ConnectionManagerDbcall {
	private IniFile ini = new IniFile();
    private static String driverName = "sun.jdbc.odbc.JdbcOdbcDriver"; //nom du driver (java 1.7 UNIQUEMENT)
    private static Connection con; //connexion
    private static String jURL = ""; //url de la baseDbcall
    private static String username = ""; //username
    private static String password = ""; //password

    public Connection getConnection() {
    	try {
    		// recuperation des variables dans le fichier .ini
    		jURL = "jdbc:odbc:"+ini.getVariable("baseDbcall","urlbaseDbcall");
			username = ini.getVariable("baseDbcall","username");
			password = ini.getVariable("baseDbcall","password");
    		try {
            	Class.forName(driverName);
            	try {
                	con = DriverManager.getConnection(jURL, username, password);
                	System.out.println("connexion reussi");
            	} catch (SQLException ex) {
                	System.out.println("Failed to create the database connection."); 
            	}
        	} catch (ClassNotFoundException ex) {
            	System.out.println("Driver not found.");
        	}
    	}
    	catch(Exception e){
    		System.out.println("erreur dans le fichier .ini");
    		e.printStackTrace();
    	}
    	return con;
    }
    
}