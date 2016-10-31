package sparql.dbpediaObjects;

/**
 * Created by Amit on 26/10/2016.
 */
public enum ARTWORK_TYPE {
    PAINTING("PAINTING", "PAINTINGS", ART_FIELD.PAINTING),
    SCULPTOR("SCULPTOR", "SCULPTORS", ART_FIELD.SCULPTING),
    PHOTOGRAPHY("PHOTOGRAPHY", "PHOTOGRAPHYS", ART_FIELD.PHOTOGRAPHY),
    PHOTO("PHOTO", "PHOTOS", ART_FIELD.PHOTOGRAPHY),
    STRUCTURE("STRUCTURE", "STRUCTURES", ART_FIELD.ARCHITECTURE),
    DRAWING("DRAWING", "DRAWINGS", ART_FIELD.DRAWING),
    SKETCH("SKETCH", "SKETCHES", ART_FIELD.SKETCHING),
    GRAFFITI("GRAFFITI", "GRAFFITI", ART_FIELD.GRAFFITI),
    BOOK("BOOK", "BOOKS", ART_FIELD.WRITING),
    FILM("FILM", "FILMS", ART_FIELD.FILM),
    MOVIE("MOVIE", "MOVIES", ART_FIELD.FILM),
    OTHER("OTHER", null, null);

    private String TypeName;
    private String TypePlural;
    private ART_FIELD TypeField;

    ARTWORK_TYPE(String typeName, String typePlural, ART_FIELD typeField) {
        TypeName = typeName;
        TypePlural = typePlural;
        TypeField = typeField;
    }

    public String getName() {
        return TypeName;
    }

    public String getTypePlural() {
        return TypePlural;
    }

    public ART_FIELD getTypeField() {
        return TypeField;
    }
}
