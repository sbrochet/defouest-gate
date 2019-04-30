package main;

import java.io.*;

import org.ini4j.*;

/**
 * 
 * permet de lire un fichier .ini
 * et de recuperer les donnees a l'interieur
 * 
 * @author florent
 * @version 1.0
 *
 */
public class IniFile {
	private Ini ini = null;
	private java.util.prefs.Preferences prefs = null;
	
	public IniFile() {
		try {
			ini = new Ini(new File("ConfigDef.ini"));
			prefs = new IniPreferences(ini);
		} catch (IOException e) {
			System.out.println("Error in IniVariableFile : "+e.getMessage().replace("ù","u"));
		}
	}
	
	
	public String getVariable(String header,String name){
		return ini.get(header, name);
	}
	
}
