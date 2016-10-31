package sparql.dbpediaObjects;

import java.util.ArrayList;

/**
 * Created by Amit on 18/10/2016.
 */
public class DBPediaArtist {

    private final String WikiPageID;
    private final String Name;
    private final String Description;
    private final ART_FIELD[] ArtFields;
    private final String[] ArtMovements;
    private final String BirthDate;
    private final String DeathDate;
    private final String[] BirthPlace;
    private final String[] DeathPlace;
    private final int TotalLinks;
    private final int TotalArt;

    public DBPediaArtist(String wikiPageID, String name, String description, String[] fields, String[] movements, String birthDate, String deathDate, String[] birthPlaces, String[] deathPlaces, int totalLinks, int totalArt) {
        WikiPageID = wikiPageID;
        Name = name;
        Description = description;
        ArrayList<ART_FIELD> artFields = new ArrayList<ART_FIELD>();
        for(String field : fields)
            artFields.add(ART_FIELD.parse(field));
        ArtFields = artFields.toArray(new ART_FIELD[]{});
        ArtMovements = movements;
        BirthDate = birthDate;
        DeathDate = deathDate;
        BirthPlace = birthPlaces;
        DeathPlace = deathPlaces;
        TotalLinks = totalLinks;
        TotalArt = totalArt;
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

    public String[] getArtFields() {
        ArrayList<String> artFields = new ArrayList<String>();
        for(ART_FIELD field : ArtFields)
            artFields.add(field.getName());
        return artFields.toArray(new String[]{});
    }

    public String[] getArtMovements() {
        return ArtMovements;
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

    public int getTotalArt() {
        return TotalArt;
    }
}
