/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is Mozilla Socorro.
 *
 * The Initial Developer of the Original Code is the Mozilla Foundation.
 * Portions created by the Initial Developer are Copyright (C) 2010
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * 
 *   Xavier Stevens <xstevens@mozilla.com>, Mozilla Corporation (original author)
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */

package com.mozilla.util;

import java.util.Calendar;

public class DateUtil {

  /**
   * Get the first moment in time for the given time and resolution
   * @param time
   * @param resolution
   * @return
   */
  public static long getTimeAtResolution(long time, int resolution) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(time);
    
    switch (resolution) {
      case Calendar.DATE:
        cal.set(Calendar.HOUR, 0);
      case Calendar.HOUR:
        cal.set(Calendar.MINUTE, 0);
      case Calendar.MINUTE:
        cal.set(Calendar.SECOND, 0);
      case Calendar.SECOND:
        cal.set(Calendar.MILLISECOND, 0);
      default:
        break;
    }
    
    return cal.getTimeInMillis();
  }

  /**
   * Get the last moment in time for the given time and resolution
   * @param time
   * @param resolution
   * @return
   */
  public static long getEndTimeAtResolution(long time, int resolution) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(time);
    
    switch (resolution) {
      case Calendar.DATE:
        cal.set(Calendar.HOUR, 23);
      case Calendar.HOUR:
        cal.set(Calendar.MINUTE, 59);
      case Calendar.MINUTE:
        cal.set(Calendar.SECOND, 59);
      case Calendar.SECOND:
        cal.set(Calendar.MILLISECOND, 999);
      default:
        break;
    }
    
    return cal.getTimeInMillis();
  }
  
}
