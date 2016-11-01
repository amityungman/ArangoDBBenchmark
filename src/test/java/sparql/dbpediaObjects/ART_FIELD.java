package sparql.dbpediaObjects;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Amit on 26/10/2016.
 */
public enum ART_FIELD {
    PAINTING("PAINTING","PAINTER","PAINTERS"),
    SCULPTING("SCULPTING","SCULPTOR","SCULPTORS"),
    PHOTOGRAPHY("PHOTOGRAPHY","PHOTOGRAPH","PHOTOGRAPHS"),
    ARCHITECTURE("ARCHITECTURE","BUILDING","BUILDINGS"),
    DRAWING("DRAWING","PAINTER","PAINTERS"),
    SKETCHING("SKETCHING","PAINTER","PAINTERS"),
    GRAFFITI("GRAFFITI","GRAFFITI ARTIST","GRAFFITI ARTISTS"),
    WRITING("WRITING","WRITER","WRITERS"),
    MUSIC("MUSIC","MUSICIAN","MUSICIANS"),
    COMPOSITION("COMPOSITION","COMPOSER","COMPOSERS"),
    SIGNING("SIGNING","SINGER","SINGERS"),
    MUSICAL_PERFORMANCE("MUSICAL_PERFORMANCE","BAND","BANDS"),
    FILM("FILM","DIRECTOR","DIRECTORS"),
    ART("ART","ARTIST","ARTISTS"),;

    private String FieldName;
    private String FieldArtistTitle;
    private String FieldArtistTitlePlural;

    ART_FIELD(String fieldName, String fieldArtistTitle, String fieldArtistTitlePlural) {
        FieldName = fieldName;
        FieldArtistTitle = fieldArtistTitle;
        FieldArtistTitlePlural = fieldArtistTitlePlural;
    }

    public String getName() {
        return FieldName;
    }

    public String getArtistTitle() {
        return FieldArtistTitle;
    }

    public String getArtistTitlePlural() {
        return FieldArtistTitlePlural;
    }

    public static ART_FIELD parse(String field) {
        for(ART_FIELD artField : ART_FIELD.values())
            if(artField.getName().equals(field.toUpperCase()))
                return artField;
        return ART_FIELD.ART;
    }

    public static List<String> getNames() {
        List<String> names = new ArrayList<String>();
        for(ART_FIELD name : ART_FIELD.values())
            names.add(name.getName());
        return names;
    }

    public static List<String> getArtistTitles() {
        List<String> titles = new ArrayList<String>();
        for(ART_FIELD title : ART_FIELD.values())
            titles.add(title.getArtistTitle());
        return titles;
    }

    public static List<String> getArtistPluralTitles() {
        List<String> titles = new ArrayList<String>();
        for(ART_FIELD title : ART_FIELD.values())
            titles.add(title.getArtistTitlePlural());
        return titles;
    }
}
