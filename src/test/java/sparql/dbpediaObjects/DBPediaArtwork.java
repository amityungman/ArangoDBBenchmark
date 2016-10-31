package sparql.dbpediaObjects;

/**
 * Created by Amit on 18/10/2016.
 */
public class DBPediaArtwork {
    private final String WikiPageID;
    private final String Name;
    private final String Description;
    private final String CreationYear;
    private final ARTWORK_TYPE ArtworkType;

    public DBPediaArtwork(String wikiPageID, String name, String description, String creationYear, ARTWORK_TYPE artworkType) {
        WikiPageID = wikiPageID;
        Name = name;
        Description = description;
        CreationYear = creationYear;
        ArtworkType = artworkType;
    }

    public DBPediaArtwork(String wikiPageID, String name, String description, String creationYear, String artworkName) {
        this(wikiPageID, name, description, creationYear, ARTWORK_TYPE.parse(artworkName));
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

    public String getCreationYear() {
        return CreationYear;
    }
}
