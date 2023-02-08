/*
 This file is part of the Yildiz-Engine project, licenced under the MIT License  (MIT)
 Copyright (c) 2023 Grégory Van den Borre
 More infos available: https://engine.yildiz-games.be
 Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to the following conditions: The above copyright
 notice and this permission notice shall be included in all copies or substantial portions of the  Software.
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 OR COPYRIGHT  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package be.yildizgames.module.database.mapping.jooq;

import be.yildizgames.module.database.DataBaseConnectionProvider;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Grégory Van den Borre
 */
public class JooqQueryHelper {

    private final System.Logger logger = System.getLogger(this.getClass().getName());

    private final DataBaseConnectionProvider connectionProvider;

    private final SQLDialect dialect;

    public JooqQueryHelper(DataBaseConnectionProvider connectionProvider) {
        super();
        this.connectionProvider = connectionProvider;
        this.dialect = getDialect(this.connectionProvider.getDriver());
    }

    private static SQLDialect getDialect(String system) {
        return switch (system) {
            case "org.postgresql.Driver" -> SQLDialect.POSTGRES;
            case "org.hsqldb.jdbc.JDBCDriver" -> SQLDialect.HSQLDB;
            default -> throw new IllegalStateException("Unsupported system: " + system);
        };
    }

    public final <T> Optional<T> select(JooqSelectOne<T> findOne) {
        try (var c = this.connectionProvider.getConnection()) {
            var context = DSL.using(c, this.dialect);
            return findOne.execute(context);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public final <T> List<T> select(JooqSelectMany<T> findMany) {
        try (var c = this.connectionProvider.getConnection()) {
            var context = DSL.using(c, this.dialect);
            return findMany.execute(context);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public final <T> Set<T> selectDistinct(JooqSelectManyDistinct<T> findMany) {
        try (var c = this.connectionProvider.getConnection()) {
            var context = DSL.using(c, this.dialect);
            return findMany.execute(context);
        } catch (SQLException e) {
            this.logger.log(System.Logger.Level.ERROR, "", e);
            throw new IllegalStateException(e);
        }
    }

    public final void execute(JooqExecutor ex) {
        try (var c = this.connectionProvider.getConnection()) {
            var context = DSL.using(c, this.dialect);
            ex.execute(context);
        } catch (SQLException e) {
            this.logger.log(System.Logger.Level.ERROR, "", e);
            throw new IllegalStateException(e);
        }
    }

    public final <T> void execute(Collection<T> objects, JooqExecutorWithParameter<T> execution) {
        try (var c = this.connectionProvider.getConnection()) {
            var context = DSL.using(c, this.dialect);
            for(var o : objects) {
                //FIXME check size and batch if necessary
                execution.execute(context, o);
            }
        } catch (SQLException | IllegalStateException e) {
            this.logger.log(System.Logger.Level.ERROR, "", e);
            throw new IllegalStateException(e);
        }
    }

    protected final <T> int executeAndGetId(T o, JooqExecutorGetId<T> execution) {
        try (var c = this.connectionProvider.getConnection()) {
            var context = DSL.using(c, this.dialect);
            return execution.execute(context, o);
        } catch (SQLException | IllegalStateException e) {
            this.logger.log(System.Logger.Level.ERROR, "", e);
            throw new IllegalStateException(e);
        }
    }

    public final int count(Table<?> table) {
        try (var c = this.connectionProvider.getConnection()) {
            var context = DSL.using(c, this.dialect);
            return context.fetchCount(table);
        } catch (SQLException e) {
            this.logger.log(System.Logger.Level.ERROR, "", e);
            throw new IllegalStateException(e);
        }
    }

}
