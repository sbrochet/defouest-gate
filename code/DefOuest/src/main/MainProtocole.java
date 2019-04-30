package main;
import java.io.*;
import java.sql.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import main.IniFile;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;

import com.opencsv.*;

public class MainProtocole {
	private static IniFile ini = new IniFile();
	private static String urlContrat = "";
	private static String filenameContrat = "";
	private static String requete = "";
	//private static String urlClient = "https://api-cc.dbcall.fr/v1/defouest/client/";
	
	public static void main(String[] args) {
		//connexion au serveur
		ConnectionManager cm = new ConnectionManager();
		Connection con = cm.getConnection();
		Boolean includeHeaders = true;
		
		// recuperation des variables dans le fichier .ini
		urlContrat = ini.getVariable("def","urlApi");
		filenameContrat = ini.getVariable("def","nom_fichier_csv"); 
		requete = ini.getVariable("base","requete");
		
		
		//creation du fichier file
		File file = null;
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(filenameContrat), '\t');
			file = new File(filenameContrat);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try{

			// Execution de la requete
			Statement jRequete = con.createStatement();
			String query = requete;
			ResultSet rs = jRequete.executeQuery(query);
			
			// ecriture du fichier file
			writer.writeAll(rs, includeHeaders);
			writer.close();
			
			//fermeture des connexion
			rs.close();
			jRequete.close();
			con.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

		//envoie du fichier au serveur
		try{
			HttpClient client = new HttpClient();
	        PostMethod postMethod = new PostMethod(urlContrat);

	        client.setConnectionTimeout(8000);

	        postMethod.setRequestBody(new FileInputStream(file));
	        postMethod.setRequestHeader("Content-type",
	            "text/csv; charset=ISO-8859-1");

	        int statusCode1 = client.executeMethod(postMethod);

	        System.out.println("statusLine>>>" + postMethod.getStatusLine());
	        postMethod.releaseConnection();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		//FIN
		System.out.println("Fin du traitement du protocole");
		Thread.currentThread().interrupt();
	}
}