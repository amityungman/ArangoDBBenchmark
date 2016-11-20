package DL;

/**
 * Created by LENOVO on 12/15/2015.
 */
public enum NODE_TYPE {
    CITY(1, "CITY"),
    STATE(2, "STATE"),
    COUNTRY(3, "COUNTRY"),
    INSTITUTE(4, "INSTITUTE"),
    MUSEUM(5, "MUSEUM"),
    NEIGHBORHOOD(6, "NEIGHBORHOOD"),
    LANDMARK(7, "LANDMARK"),
    CONTINENT(8, "CONTINENT"),
    ARTICLE(9, "ARTICLE"),
    UNKNOWN(10, "UNKNOWN"),
    TCT(11, "TCT"),
    HOTEL(12, "HOTEL"),
    DISTRICT(13, "DISTRICT"),
    IMAGE(14, "IMAGE"),
    VIDEO(15, "VIDEO"),
    RESTAURANT(16, "RESTAURANT"),
    TOPIC(17, "TOPIC"),
    USER(18, "USER"),
    SEARCH_PATTERN(19, "SEARCH_PATTERN"),
    GENERIC(20, "GENERIC"),
    PARAGRAPH(21, "PARAGRAPH"),
    GENRE(22, "GENRE"),
    WORLD(23, "WORLD"),
    AUTHOR(24, "AUTHOR"),
    ARTIST(25, "ARTIST"),
    ARTWORK(26, "ARTWORK"),
    GALLERY(27, "GALLERY"),
    DIET(28, "DIET"),
    CUISINE(29, "CUISINE"),
    BAR(30, "BAR"),
    CAFE(31, "CAFE"),
    FAST_FOOD(32, "FAST_FOOD"),
    FOOD_COURT(33, "FOOD_COURT"),
    ICE_CREAM(34, "ICE_CREAM"),
    PUB(35, "PUB"),
    GROUP_OF_COUNTRIES(36, "GROUP_OF_COUNTRIES"),
    GROUP_OF_ISLANDS(37, "GROUP_OF_ISLANDS"),
    ISLAND(38, "ISLAND"),
    NATIONAL_PARK(40, "NATIONAL_PARK"),
    FOOD_ESTABLISHMENT(41, "FOOD_ESTABLISHMENT"),
    FOOD_ESTABLISHMENT_TYPE(42, "FOOD_ESTABLISHMENT_TYPE"),
    STREET(43, "STREET"),
    CHEF(44, "CHEF"),
    FOOD_ESTABLISHMENT_ATTRIBUTE(46, "FOOD_ESTABLISHMENT_ATTRIBUTE"),
    FOOD_ESTABLISHMENT_SERVICE(47, "FOOD_ESTABLISHMENT_SERVICE"),
    ART_MOVEMENT(48, "ART_MOVEMENT"),
    ART_FIELD(49, "ART_FIELD"),
    EXHIBITION(50, "EXHIBITION"),
    MUSICIAN(51, "MUSICIAN"),
    MUSICIAN_TYPE(52, "MUSICIAN_TYPE"),
    MUSIC_WORK(53, "MUSIC_WORK"),
    MUSIC_VENUE(54, "MUSIC_VENUE"),
    VENUE_TYPE(55, "VENUE_TYPE"),
    MUSIC_WORK_TYPE(56, "MUSIC_WORK_TYPE"),
    MUSIC_GENRE(57, "MUSIC_GENRE");


    public int getValue() {
        return value;
    }

    private int value;
    private String name;

    NODE_TYPE(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public static NODE_TYPE fromInteger(int type) {
        for (NODE_TYPE nodeType : NODE_TYPE.values()) {
            if (type == nodeType.getValue()) {
                return nodeType;
            }
        }
        return null;
    }

    NODE_TYPE(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public static NODE_TYPE fromString(String name) {
        if (name != null) {
            for (NODE_TYPE b : NODE_TYPE.values()) {
                if (name.equalsIgnoreCase(b.name)) {
                    return b;
                }
            }
        }
        return null;
    }

    public static NODE_TYPE parse(String name) {
        if (name != null) {
            for (NODE_TYPE b : NODE_TYPE.values()) {
                if (name.equalsIgnoreCase(b.name)) {
                    return b;
                }
            }
        }
        return UNKNOWN;
    }

    public static boolean contains(String test) {

        for (NODE_TYPE c : NODE_TYPE.values()) {
            if (c.name().equals(test)) {
                return true;
            }
        }

        return false;
    }

    public static Boolean isLocationNode(NODE_TYPE nodesType) {


        return nodesType.equals(CITY) || nodesType.equals(STATE) || nodesType.equals(COUNTRY) || nodesType.equals(NEIGHBORHOOD) || nodesType.equals(CONTINENT) || nodesType.equals(DISTRICT);

    }

}
