/*##############################################################################

Copyright (C) 2011 HPCC Systems.

All rights reserved. This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

package org.hpccsystems.jdbcdriver;

import java.util.ArrayList;
import java.util.List;


public class SQLOperator
{
    private static List<String> validOps;

    // When adding a new operator, make sure to add it to validOps array
    public static final String  eq        = new String("=");
    public static final String  neq       = new String("<>");
    public static final String  neq2      = new String("!=");
    public static final String  isNull    = new String(" IS NULL ");
    public static final String  isNotNull = new String(" IS NOT NULL ");
    public static final String  gt        = new String(">");
    public static final String  lt        = new String("<");
    public static final String  gte       = new String(">=");
    public static final String  lte       = new String("<=");
    public static final String  and       = new String(" AND ");
    public static final String  or        = new String(" OR ");
    public static final String  not       = new String(" NOT ");
    public static final String  exists    = new String(" EXISTS ");
    public static final String  like      = new String(" LIKE ");
    public static final String  in        = new String(" IN ");
    public static final String  notIn     = new String(" NOT IN ");

    static
    {
        validOps = new ArrayList<String>();
        validOps.add(eq);
        validOps.add(neq);
        validOps.add(neq2);
        validOps.add(isNull);
        validOps.add(isNotNull);
        validOps.add(gt);
        validOps.add(lt);
        validOps.add(gte);
        validOps.add(lte);
        validOps.add(and);
        validOps.add(or);
        validOps.add(not);
        validOps.add(exists);
        validOps.add(like);
        validOps.add(in);
        validOps.add(notIn);
    }

    private final String        value;

    public SQLOperator(String operator)
    {
        if (validOps.contains(operator.toUpperCase()))
            value = operator.toUpperCase();
        else
            value = null;
    }

    static public String parseOperatorFromFragmentStr(String fragment)
    {
        String trimmedFragment = fragment.trim();
        String operator = null;

        if (trimmedFragment.indexOf(SQLOperator.gte) != -1)
            operator = SQLOperator.gte;
        else if (trimmedFragment.indexOf(SQLOperator.lte) != -1)
            operator = SQLOperator.lte;
        else if (trimmedFragment.indexOf(SQLOperator.neq) != -1)
            operator = SQLOperator.neq;
        else if (trimmedFragment.indexOf(SQLOperator.neq2) != -1)
            operator = SQLOperator.neq2;
        else if (trimmedFragment.indexOf(SQLOperator.eq) != -1)
            operator = SQLOperator.eq;
        else if (trimmedFragment.indexOf(SQLOperator.gt) != -1)
            operator = SQLOperator.gt;
        else if (trimmedFragment.indexOf(SQLOperator.lt) != -1)
            operator = SQLOperator.lt;
        else if (trimmedFragment.toUpperCase().indexOf(SQLOperator.notIn) != -1)
            operator = SQLOperator.notIn;
        else if (trimmedFragment.toUpperCase().indexOf(SQLOperator.in) != -1)
            operator = SQLOperator.in;

        return operator;
    }

    public String getValue()
    {
        return value;
    }

    public boolean isValid()
    {
        return validOps.contains(value);
    }

    @Override
    public String toString()
    {
        return value == null ? "" : value;
    }
}
