package main;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import main.IniFile;
import org.json.JSONException;
import org.json.JSONObject;

public class MainProtocole {
	private static IniFile ini = new IniFile();
	private static FileWriter file;
	
	public static void main(String[] args) {
		String tableName = "";
		String primarykey = "";
		int max= 0;

		//connexion au serveur dbcall
		ConnectionManagerDbcall cmDbcall = new ConnectionManagerDbcall();
		Connection conDbcall = cmDbcall.getConnection();
	
		//connexion au serveur
		ConnectionManager cm = new ConnectionManager();
		Connection con = cm.getConnection();
				

		try{
			//creation Statement
			Statement jRequete = con.createStatement();
			
			//Recuperation de la cle primaire
			DatabaseMetaData dbmd = con.getMetaData();
			try (ResultSet tables = dbmd.getTables(null, null, "Contrat", new String[] { "TABLE" })) {
			    while (tables.next()) {
			        String catalog = tables.getString("TABLE_CAT");
			        String schema = tables.getString("TABLE_SCHEM");
			        String table = tables.getString("TABLE_NAME");
			        System.out.println("Table: " + table);
			        tableName=table;
			        try (ResultSet primaryKeys = dbmd.getPrimaryKeys(catalog, schema, table)) {
			            while (primaryKeys.next()) {
			                primarykey = primaryKeys.getString("COLUMN_NAME");
			                System.out.println("Primary key: " + primarykey);
			            }
			        }
			        catch(Exception e) {
						System.out.println("erreur : getPrimaryKeys");
						System.out.println(e.getMessage());
					}
			    }
			}
			catch(Exception e) {
				System.out.println("erreur : getTables");
				System.out.println(e.getMessage());
			}
			
			//Recuperation de la valeur max de la cle primaire
			try {
				String queryMax = "SELECT MAX("+primarykey+") FROM "+tableName;
				ResultSet rsMax = jRequete.executeQuery(queryMax);
				rsMax.next();
				max = Integer.parseInt(rsMax.getString("Expr1"));
				System.out.println("MAX VALUE OF : "+primarykey+" , FROM TABLE : "+tableName+" , IS : "+max);
				//creation du json local
				JSONObject jsonLocal = new JSONObject();
				jsonLocal.put("primarykey", primarykey);
				jsonLocal.put("table", tableName);
				jsonLocal.put("max", max);
				//ecriture du json local dans un fichier
				try {
		            // Constructs a FileWriter given a file name, using the platform's default charset
		            file = new FileWriter("./jsonLocal.json");
		            file.write(jsonLocal.toString());
		        } catch (IOException e) {
		            e.printStackTrace();
		        } finally {
		            try {
		                file.flush();
		                file.close();
		            } catch (IOException e) {
		                e.printStackTrace();
		            }
		        }
				//fermeture 
				rsMax.close();
			}catch(Exception e) {
				System.out.println("erreur : SELECT MAX VALUE");
				System.out.println(e.getMessage());
			}
			
			//execution requete et envoie vers dbcall
			String query = ini.getVariable("base","requete");
			ResultSet rs = jRequete.executeQuery(query);
			sendJsonToDbcall(rs,conDbcall);
			
			//fermeture des connexion
			rs.close();
			jRequete.close();
			con.close();
			conDbcall.close();
			

			System.out.println("Fin du traitement du protocole sans erreur");
		}
		catch(Exception e){
			System.out.println("erreur : fin du traitement du protocole");
			e.printStackTrace();
		}

		
		
		//FIN
		Thread.currentThread().interrupt();
	}
	
	//Envoie un json vers la bdd Dbcall
	public static void sendJsonToDbcall( ResultSet rs, Connection con )
		    throws SQLException, JSONException
		  {
		    ResultSetMetaData rsmd = rs.getMetaData();

		    while(rs.next()) {
		      int numColumns = rsmd.getColumnCount();
		      JSONObject obj = new JSONObject();

		      for (int i=1; i<numColumns+1; i++) {
		        String column_name = rsmd.getColumnName(i);

		        if(rsmd.getColumnType(i)==java.sql.Types.ARRAY){
		         obj.put(column_name, rs.getArray(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.BIGINT){
		         obj.put(column_name, rs.getInt(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.BOOLEAN){
		         obj.put(column_name, rs.getBoolean(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.BLOB){
		         obj.put(column_name, rs.getBlob(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.DOUBLE){
		         obj.put(column_name, rs.getDouble(column_name)); 
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.FLOAT){
		         obj.put(column_name, rs.getFloat(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.INTEGER){
		         obj.put(column_name, rs.getInt(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.NVARCHAR){
		         obj.put(column_name, rs.getNString(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.VARCHAR){
		         obj.put(column_name, rs.getString(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.TINYINT){
		         obj.put(column_name, rs.getInt(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.SMALLINT){
		         obj.put(column_name, rs.getInt(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.DATE){
		         obj.put(column_name, rs.getDate(column_name));
		        }
		        else if(rsmd.getColumnType(i)==java.sql.Types.TIMESTAMP){
		        obj.put(column_name, rs.getTimestamp(column_name));   
		        }
		        else{
		         obj.put(column_name, rs.getObject(column_name));
		        }
		      }
		    //insertion dans la bdd de Dbcall
		    PreparedStatement pstmt = con.prepareStatement("INSERT into public.sites(flux) values (?::JSON)");
			pstmt.setObject(1, obj.toString());
			pstmt.executeUpdate();
			//fermeture
			pstmt.close();
		    }
		  }
	
}