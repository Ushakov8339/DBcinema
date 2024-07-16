import java.sql.*;

public class JDBCRunner {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // Компьютер + порт по умолчанию
    private static final String DATABASE_NAME = "cinema_base";          // FIXME имя базы
    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных
    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            getDirector(connection);System.out.println();
            getGenres(connection);System.out.println();
            getStudio(connection);System.out.println();
            getFilms(connection);System.out.println();
            getTopFiveHighRate(connection);System.out.println();

            // TODO show with param
            getFilmInfo(connection, "Шрэк");System.out.println();
            getFilmsByGenre(connection, "Ужасы");System.out.println();
            getFilmsByDuration(connection, 90, 160);System.out.println();
            getFilmsByDirector(connection, "Квентин", "Тарантино");System.out.println();

            // TODO correction
            addDirector(connection, "Джон", "Малкович");System.out.println();
            correctDirector(connection, "Джон", "Мактирнан");System.out.println();
            deleteDirector(connection, "Джон", "Мактирнан");System.out.println();
            addStudio(connection, "Strong Heart", "Россия", 1970);System.out.println();
            correctStudio(connection, "Strong Heart", "США", 1990);System.out.println();
            deleteStudio(connection, "Strong Heart");System.out.println();


        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных)
            // возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    // Проверка окружения и доступа к базе данных

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println(
                    "Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    private static void getDirector(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT id, name, surname FROM director;");
        System.out.println("Режиссеры:");
        while (rs.next()) {
            System.out.println( rs.getInt(1) + " | " +
                                rs.getString(2) + " " +
                                rs.getString(3));
        }
    }

    static void getGenres(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT id, title FROM genre;");
        System.out.println("Жанры:");
        while (rs.next()) {
            System.out.println(rs.getInt(1) + " | " + rs.getString(2));
        }
    }

    static void getStudio(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT id, name, country, year FROM studio;");
        System.out.println("Студии:");
        while (rs.next()) {
            System.out.println(
                            rs.getInt(1) + " | " +
                            rs.getString(2) + " | " +
                            rs.getString(3) + " | " +
                            rs.getInt(4));
        }
    }

    static void getFilms(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(
                    "SELECT id, title, id_genre, time_min, id_studio, " +
                        "release_year, id_director, my_rating " +
                        "FROM films;");
        System.out.println("Фильмы:");
        while (rs.next()) {
            System.out.println(
                            rs.getInt(1) + " | " + rs.getString(2) + " | " +
                            rs.getInt(3) + " | " + rs.getInt(4) + " | " +
                            rs.getInt(5) + " | " + rs.getInt(6) + " | " +
                            rs.getInt(7) + " | " + rs.getInt(8));
        }
    }

    private static void getTopFiveHighRate(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(
                    "SELECT films.id, films.title, films.my_rating " +
                        "FROM films " + "WHERE films.my_rating >= 8 " +
                        "ORDER BY films.my_rating DESC " + "LIMIT 5");
        System.out.println("Топ 5 фильмов с высоким рейтингом:");
        while (rs.next()) {
            System.out.println(
                    rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3));
        }
    }

    private static void getFilmInfo(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;
        name = '%' + name + '%';
        PreparedStatement statement = connection.prepareStatement(
                    "SELECT films.id, films.title, genre.title, films.time_min, studio.name, " + "films.release_year, " +
                        "director.name || ' ' || director.surname AS \"Full name\", films.my_rating " +
                        "FROM films " + "JOIN genre ON genre.id = films.id_genre " +
                        "JOIN studio ON studio.id = films.id_studio " +
                        "JOIN director ON director.id = films.id_director " +
                        "WHERE films.title LIKE ?;");
        statement.setString(1, name);
        ResultSet rs = statement.executeQuery();
        System.out.println("Информация о фильме:");
        while (rs.next()) {
            System.out.println(
                            rs.getInt(1) + " | " + rs.getString(2) + " | " +
                            rs.getString(3) + " | " + rs.getInt(4) + " | " +
                            rs.getString(5) + " | " + rs.getInt(6) + " | " +
                            rs.getString(7) + " |" + rs.getInt(8));
        }
    }

    private static void getFilmsByGenre(Connection connection, String title) throws SQLException {
        if (title == null || title.isBlank()) return;
        title = '%' + title + '%';
        PreparedStatement statement = connection.prepareStatement(
                    "SELECT films.id, films.title, genre.title " +
                        "FROM films " + "JOIN genre ON genre.id = films.id_genre " +
                        "WHERE genre.title LIKE ?;");
        statement.setString(1, title);
        ResultSet rs = statement.executeQuery();
        System.out.println("Фильмы по жанру:");
        while (rs.next()) {
            System.out.println(
                            rs.getInt(1) + " | " +
                            rs.getString(2) + " | " +
                            rs.getString(3));
        }
    }

    private static void getFilmsByDirector(Connection connection, String name, String surname) throws SQLException {
        if (name == null || name.isBlank() || surname == null || surname.isBlank()) return;
        name = '%' + name + '%';
        surname = '%' + surname + '%';
        PreparedStatement statement = connection.prepareStatement(
                    "SELECT films.id, films.title, director.name || ' ' || director.surname AS \"Full name\" " +
                        "FROM films " + "JOIN director ON director.id = films.id_director " +
                        "WHERE director.name LIKE ? AND director.surname LIKE ?");
        statement.setString(1, name);
        statement.setString(2, surname);
        ResultSet rs = statement.executeQuery();
        System.out.println("Фильмы по режиссеру:");
        while (rs.next()) {
            System.out.println(
                            rs.getInt(1) + " | " +
                            rs.getString(2) + " | " +
                            rs.getString(3));
        }
    }

    private static void getFilmsByDuration(Connection connection, int fTime, int sTime) throws SQLException {
        if (fTime < 0 || sTime < 0) return;
        PreparedStatement statement = connection.prepareStatement(
                "SELECT films.id, films.title, films.time_min " +
                        "FROM films " +
                        "WHERE films.time_min BETWEEN ? AND ?;");
        statement.setInt(1, fTime);
        statement.setInt(2, sTime);
        ResultSet rs = statement.executeQuery();
        System.out.println("Фильмы по продолжительности от " + fTime + " до " + sTime + " минут:");
        while (rs.next()) {
            System.out.println(
                            rs.getInt(1) + " | " +
                            rs.getString(2) + " | " +
                            rs.getInt(3));
        }
    }

    private static void addDirector(Connection connection, String name, String surname) throws SQLException {
        if (name == null || name.isBlank() || surname == null || surname.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO director(name, surname) VALUES (?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setString(2, surname);
        int count = statement.executeUpdate();
        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор режиссера " + rs.getInt(1));
        }
        System.out.println("INSERTED " + count + " director");
        getDirector(connection);
    }

    private static void addStudio(Connection connection, String name, String country, int year) throws SQLException {
        if (name == null || name.isBlank() || country == null || country.isBlank() || year < 0) return;
        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO studio(name, country, year) VALUES (?,?,?) returning id;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setString(2, country);
        statement.setInt(3, year);
        int count = statement.executeUpdate();
        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор студии " + rs.getInt(1));
        }
        System.out.println("INSERTED " + count + " studio");
        getStudio(connection);
    }

    private static void deleteDirector(Connection connection, String name, String surname) throws SQLException {
        if (name == null || name.isBlank() || surname == null || surname.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM director WHERE name=? AND surname=?;");
        statement.setString(1, name);
        statement.setString(2, surname);
        int count = statement.executeUpdate();
        System.out.println("DELETED " + count + " directors");
        getDirector(connection);
    }

    private static void deleteStudio(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM studio WHERE name=?;");
        statement.setString(1, name);
        int count = statement.executeUpdate();
        System.out.println("DELETED " + count + " studios");
        getStudio(connection);
    }

    private static void correctDirector(Connection connection, String name, String surname) throws SQLException {
        if (name == null || name.isBlank() || surname == null || surname.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement(
                "UPDATE director SET name=?, surname=? WHERE name=? OR surname=?;");
        statement.setString(1, name);
        statement.setString(2, surname);
        statement.setString(3, name);
        statement.setString(4, surname);
        int count = statement.executeUpdate();
        System.out.println("UPDATED " + count + " directors");
        getDirector(connection);
    }

    private static void correctStudio(Connection connection, String name, String country, int year) throws SQLException {
        if (name == null || name.isBlank() || country == null || country.isBlank() || year < 0) return;
        PreparedStatement statement = connection.prepareStatement(
                "UPDATE studio SET name=?, country=?, year=? WHERE name=?;");
        statement.setString(1, name);
        statement.setString(2, country);
        statement.setInt(3, year);
        statement.setString(4, name);
        int count = statement.executeUpdate();
        System.out.println("UPDATED " + count + " studios");
        getStudio(connection);
    }
}