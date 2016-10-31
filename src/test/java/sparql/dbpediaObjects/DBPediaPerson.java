package sparql.dbpediaObjects;

import java.util.ArrayList;

/**
 * Created by Amit on 30/10/2016.
 */
public class DBPediaPerson {
    private final String WikiPageID;
    private final String Name;
    private final String Description;
    private final String BirthDate;
    private final String DeathDate;
    private final String[] BirthPlace;
    private final String[] DeathPlace;
    private final int TotalLinks;

    public DBPediaPerson(String wikiPageID, String name, String description, String birthDate, String deathDate, String[] birthPlaces, String[] deathPlaces, int totalLinks) {
        WikiPageID = wikiPageID;
        Name = name;
        Description = description;
        ArrayList<ART_FIELD> artFields = new ArrayList<ART_FIELD>();
        BirthDate = birthDate;
        DeathDate = deathDate;
        BirthPlace = birthPlaces;
        DeathPlace = deathPlaces;
        TotalLinks = totalLinks;
    }

    public String getWikiPageID() {
        return WikiPageID;
    }

    public String getName() {
        return Name;
    }

    public String getDescription() {
        return Description;
    }

    public String getBirthDate() {
        return BirthDate;
    }

    public String getDeathDate() {
        return DeathDate;
    }

    public String[] getBirthPlace() {
        return BirthPlace;
    }

    public String[] getDeathPlace() {
        return DeathPlace;
    }

    public int getTotalLinks() {
        return TotalLinks;
    }
}
