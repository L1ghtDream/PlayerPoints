package org.black_ixx.playerpoints.storage.models;

import org.black_ixx.playerpoints.PlayerPoints;
import org.black_ixx.playerpoints.config.RootConfig;
import org.black_ixx.playerpoints.storage.DatabaseStorage;

import java.sql.*;
import java.util.Collection;
import java.util.HashSet;
import java.util.logging.Level;

/**
 * Storage handler for MySQL source.
 *
 * @author Mitsugaru
 */
public class MySQLStorage extends DatabaseStorage {

    /**
     * MYSQL reference.
     */
    //private MySQL mysql;
    private static Statement statement;
    private static PreparedStatement preparedStatement;
    private static Connection connection;
    /**
     * The table name to use.
     */
    private final String tableName;
    /**
     * Skip operation flag.
     */
    private final boolean skip = false;
    /**
     * Number of attempts to reconnect before completely failing an operation.
     */
    private int retryLimit = 10;
    /**
     * Current retry count.
     */
    private int retryCount = 0;

    /**
     * Constructor.
     *
     * @param plugin - Plugin instance.
     */
    public MySQLStorage(PlayerPoints plugin) {
        super(plugin);
        RootConfig config = plugin.getModuleForClass(RootConfig.class);
        if (config.debugDatabase) {
            plugin.getLogger().info("Constructor");
        }
        retryLimit = config.retryLimit;
        //setup table name and strings
        tableName = config.table;
        SetupQueries(tableName);
        //Connect
        connect();
    }

    @Override
    public int getPoints(String id) {
        int points = 0;
        RootConfig config = plugin.getModuleForClass(RootConfig.class);
        if (id == null || id.equals("")) {
            if (config.debugDatabase) {
                plugin.getLogger().info("getPoints() - bad ID");
            }
            return points;
        }
        if (config.debugDatabase) {
            plugin.getLogger().info("getPoints(" + id + ")");
        }
        PreparedStatement statement = null;
        ResultSet result = null;
        try {
            statement = connection.prepareStatement(String.format(GET_POINTS, tableName));
            statement.setString(1, id);
            result = statement.executeQuery();
            if (result != null && result.next()) {
                points = result.getInt("points");
            }
        } catch (SQLException e) {
            plugin.getLogger().log(Level.SEVERE,
                    "Could not create getter statement.", e);
            retryCount++;
            connect();
            if (!skip) {
                points = getPoints(id);
            }
        } finally {
            cleanup(result, statement);
        }
        retryCount = 0;
        if (config.debugDatabase) {
            plugin.getLogger().info("getPlayers() result - " + points);
        }
        return points;
    }

    @Override
    public boolean setPoints(String id, int points) {
        boolean value = false;
        RootConfig config = plugin.getModuleForClass(RootConfig.class);
        if (id == null || id.equals("")) {
            if (config.debugDatabase) {
                plugin.getLogger().info("setPoints() - bad ID");
            }
            return value;
        }
        if (config.debugDatabase) {
            plugin.getLogger().info("setPoints(" + id + "," + points + ")");
        }
        final boolean exists = playerEntryExists(id);
        PreparedStatement statement = null;
        ResultSet result = null;
        try {
            if (exists) {
                statement = connection.prepareStatement(String.format(UPDATE_PLAYER, tableName));
            } else {
                statement = connection.prepareStatement(String.format(INSERT_PLAYER, tableName));
            }
            statement.setInt(1, points);
            statement.setString(2, id);
            statement.executeUpdate();
            value = true;
        } catch (SQLException e) {
            plugin.getLogger().log(Level.SEVERE,
                    "Could not create setter statement.", e);
            retryCount++;
            connect();
            if (!skip) {
                value = setPoints(id, points);
            }
        } finally {
            cleanup(result, statement);
        }
        retryCount = 0;
        if (config.debugDatabase) {
            plugin.getLogger().info("setPoints() result - " + value);
        }
        return value;
    }

    @Override
    public boolean playerEntryExists(String id) {
        boolean has = false;
        RootConfig config = plugin.getModuleForClass(RootConfig.class);
        if (id == null || id.equals("")) {
            if (config.debugDatabase) {
                plugin.getLogger().info("playerEntryExists() - bad ID");
            }
            return has;
        }
        if (config.debugDatabase) {
            plugin.getLogger().info("playerEntryExists(" + id + ")");
        }
        PreparedStatement statement = null;
        ResultSet result = null;
        try {
            statement = connection.prepareStatement(String.format(GET_POINTS, tableName));
            statement.setString(1, id);
            result = statement.executeQuery();
            if (result.next()) {
                has = true;
            }
        } catch (SQLException e) {
            plugin.getLogger().log(Level.SEVERE,
                    "Could not create player check statement.", e);
            retryCount++;
            connect();
            if (!skip) {
                has = playerEntryExists(id);
            }
        } finally {
            cleanup(result, statement);
        }
        retryCount = 0;
        if (config.debugDatabase) {
            plugin.getLogger().info("playerEntryExists() result - " + has);
        }
        return has;
    }

    @Override
    public boolean removePlayer(String id) {
        boolean deleted = false;
        if (id == null || id.equals("")) {
            return deleted;
        }
        PreparedStatement statement = null;
        ResultSet result = null;
        RootConfig config = plugin.getModuleForClass(RootConfig.class);
        if (config.debugDatabase) {
            plugin.getLogger().info("removePlayers(" + id + ")");
        }
        try {
            statement = connection.prepareStatement(String.format(REMOVE_PLAYER, tableName));
            statement.setString(1, id);
            result = statement.executeQuery();
            deleted = true;
        } catch (SQLException e) {
            plugin.getLogger().log(Level.SEVERE,
                    "Could not create player remove statement.", e);
            retryCount++;
            connect();
            if (!skip) {
                deleted = playerEntryExists(id);
            }
        } finally {
            cleanup(result, statement);
        }
        retryCount = 0;
        if (config.debugDatabase) {
            plugin.getLogger().info("renovePlayers() result - " + deleted);
        }
        return deleted;
    }

    @Override
    public Collection<String> getPlayers() {
        Collection<String> players = new HashSet<String>();

        RootConfig config = plugin.getModuleForClass(RootConfig.class);
        if (config.debugDatabase) {
            plugin.getLogger().info("Attempting getPlayers()");
        }
        PreparedStatement statement = null;
        ResultSet result = null;
        try {
            preparedStatement = connection.prepareStatement(String.format(GET_PLAYERS, tableName));
            result = preparedStatement.executeQuery();

            while (result.next()) {
                String name = result.getString("playername");
                if (name != null) {
                    players.add(name);
                }
            }
        } catch (SQLException e) {
            plugin.getLogger().log(Level.SEVERE,
                    "Could not create get players statement.", e);
            retryCount++;
            connect();
            if (!skip) {
                players.clear();
                players.addAll(getPlayers());
            }
        } finally {
            cleanup(result, statement);
        }
        retryCount = 0;
        if (config.debugDatabase) {
            plugin.getLogger().info("getPlayers() result - " + players.size());
        }
        return players;
    }

    /**
     * Connect to MySQL database. Close existing connection if one exists.
     */
    private void connect() {

        RootConfig config = plugin.getModuleForClass(RootConfig.class);

        try {
            synchronized (plugin) {
                if (connection != null && connection.isClosed()) {
                    return;
                }

                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection("jdbc:mysql://" + config.host + ":" + config.port + "/?autoReconnect=true", config.user, config.password);
                statement = connection.createStatement();
                statement.executeUpdate("USE " + config.database);
                System.out.println("Connection successful");
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (id INT UNSIGNED NOT NULL AUTO_INCREMENT, playername varchar(36) NOT NULL, points INT NOT NULL, PRIMARY KEY(id), UNIQUE(playername))");
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean destroy() {
        boolean success = false;
        RootConfig config = plugin.getModuleForClass(RootConfig.class);
        if (config.debugDatabase) {
            plugin.getLogger().info("Dropping playerpoints table");
        }
        try {
            statement.executeUpdate("DROP TABLE " + tableName);
            success = true;
        } catch (SQLException e) {
            plugin.getLogger().log(Level.SEVERE, "Could not drop MySQL table.", e);
        }
        return success;
    }

    @Override
    public boolean build() {
        boolean success = false;
        RootConfig config = plugin.getModuleForClass(RootConfig.class);
        if (config.debugDatabase) {
            plugin.getLogger().info(String.format("Creating %s table", tableName));
        }
        try {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (id INT UNSIGNED NOT NULL AUTO_INCREMENT, playername varchar(36) NOT NULL, points INT NOT NULL, PRIMARY KEY(id), UNIQUE(playername))");
            success = true;
        } catch (SQLException e) {
            plugin.getLogger().log(Level.SEVERE,
                    "Could not create MySQL table.", e);
        }
        return success;
    }

}
