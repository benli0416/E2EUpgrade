CREATE OR REPLACE FUNCTION INTERFACE.lotus_intf_out_sale2e_asc (
   trans_code_in                       VARCHAR2 := 'SALE2E',
   business_unit_id_in                 VARCHAR2 := '88',
   site_id_in                          VARCHAR2 := '000',
   loc_dir_in                          VARCHAR2 := 'LOTUS_PE2E',
   loc_bak_dir_in                      VARCHAR2 := 'LOTUS_PE2E_HISTORY'
)
   RETURN VARCHAR2
IS
   v_flag_server                 VARCHAR2 (7) := 'PRD';
   n_count                       NUMBER (16);
   v_count                       NUMBER (16);
   t_errcode                     NUMBER (5);
   t_errmsg                      VARCHAR2 (200);
   trans_ts                      DATE := SYSDATE;
   ft                            UTL_FILE.file_type;
   ft_bak                        UTL_FILE.file_type;
   ft_ok                         UTL_FILE.file_type;
   --cseq             varchar2(12);
   file_name                     VARCHAR2 (500);
   file_line                     NUMBER (16) := 0;
   d_week_ending_date            DATE;
   v_po_method                   VARCHAR2 (10);
   v_vendor_id                   vendors.vendor_id%TYPE;
   v_bu                          business_units.business_unit_id%TYPE;
   dummy                         VARCHAR2 (1);
   v_notfound                    BOOLEAN;
   v_site_id                     style_vendors.site_id%TYPE;
   v_site_name                   sites.NAME%TYPE := '';
   --v_region_id        style_vendors.site_group_id%TYPE;
   v_style_id                    styles.style_id%TYPE;
   v_year                        merchandising_calendars.merchandising_year%TYPE;
   v_week                        merchandising_calendars.merchandising_week%TYPE;
   v_style_desc                  styles.description%TYPE;
   v_old_vendor_id               vendors.vendor_id%TYPE;
   v_old_style_id                styles.style_id%TYPE;

   CURSOR cur_vendor
   IS
      SELECT a.business_unit_id,
             a.add_info_key_1_data vendor_id
      FROM   add_info_data a,
             merch.add_info_property b
      WHERE  a.business_unit_id = business_unit_id_in
      AND    b.business_unit_id = a.business_unit_id
      AND    b.add_info_table_name = 'VENDOR'
      AND    b.add_info_field_name = 'EXCHANGE_SELLINV_IND'
      AND    b.add_info_table_name = a.add_info_table_name
      AND    b.add_info_id = a.add_info_id
      AND    a.add_info_data = 'Y';

   -- The SQL is from Neng's Function Spec.2006.1.20
   -- region_id used only for "order by"
   CURSOR cur_style
   IS
      SELECT   sr.site_group_id region_id,
               a.site_id,
               a.style_id,
               SUM (NVL (sales_regular, 0)) + SUM (NVL (sales_markdown, 0))
                                                                    sales_amt,
                 SUM (NVL (qty_sold_regular, 0))
               + SUM (NVL (qty_sold_markdown, 0)) sales_qty,
               SUM (NVL (sales_cost, 0)) COST
      FROM     lt_site_cost_prices a,
               site_group_details sr,
               lotus.lotus_style_weekly_stats l
      WHERE    1 = 1
      AND      a.business_unit_id = business_unit_id_in
      AND      a.vendor_id = v_vendor_id
      AND      a.business_unit_id = l.business_unit_id
      AND      a.site_id = l.site_id
      AND      a.style_id = l.style_id
      AND      l.business_unit_id = sr.business_unit_id
      AND      l.site_id = sr.site_id
      AND      sr.sub_type = 'REGION'
      AND      l.site_id NOT IN (SELECT site_id
                                 FROM   sites
                                 WHERE  sub_type = 'W')
      AND      lt_get_site_vendor_info (business_unit_id_in,
                                        'SITE',
                                        NULL,
                                        a.site_id,
                                        v_vendor_id,
                                        'PO_SENDING_METHOD'
                                       ) = 'W'
      AND      l.merchandising_year = v_year
      AND      l.merchandising_week = v_week
      GROUP BY a.style_id,
               sr.site_group_id,
               a.site_id
      ORDER BY a.style_id,
               sr.site_group_id,
               a.site_id;

   CURSOR cur_iro
   IS
      SELECT   record_type,
               vendor_id,
               sales_report_date,
               start_date,
               end_date,
               style_id,
               style_description,
               site_id,
               site_name,
               sales_qty,
               sales_amt,
               gp_amt,
               line_no,
               business_unit_id,
               section_id
      FROM     lotus_intf_sales_e2e a
      WHERE    a.business_unit_id = business_unit_id_in
      AND      a.end_date = d_week_ending_date
      AND      po_method = 'W'
      ORDER BY vendor_id,
               NVL (style_id, '00000000'),
               DECODE (record_type,
                       'H', '1',
                       'D', '2',
                       'S', '3'
                      ),
               (SELECT MAX (v.region_id)
                FROM   v_site_hierarchy v
                WHERE  v.site_id = a.site_id),
               site_id;
BEGIN
   n_count := 0;
   file_line := 0;
   v_count := 0;

   SELECT MAX (m.week_ending_date),
          MAX (m.merchandising_year),
          MAX (m.merchandising_week)
   INTO   d_week_ending_date,
          v_year,
          v_week
   FROM   merchandising_calendars m
   WHERE  m.business_unit_id = business_unit_id_in
   AND    m.week_ending_date BETWEEN TRUNC (SYSDATE) - 7 AND   TRUNC (SYSDATE)
                                                             - 1;

   SELECT COUNT (*)
   INTO   n_count
   FROM   lotus_intf_sales_e2e a
   WHERE  a.business_unit_id = business_unit_id_in
   AND    a.end_date = d_week_ending_date
   AND    a.po_method = 'W'
   AND    ROWNUM = 1;

   SELECT COUNT (*)
   INTO   v_count
   FROM   lotus_intf_sales_e2e a
   WHERE  a.business_unit_id = business_unit_id_in
   AND    a.end_date = d_week_ending_date
   AND    a.po_method = 'W'
   AND    ROWNUM = 1;

   IF v_count <> 1
   THEN
      RETURN ('0');
   END IF;

   /*select to_char(interface.seq_interface.nextval,'FM000000000')
           into cseq from dual where rownum=1;*/
   --file_name:=to_char(business_unit_id_in,'FM00')||'_ldx_'||cseq||'_SALEDX'||'.dat';
   SELECT COUNT (*)
   INTO   file_line
   FROM   lotus_intf_sales_e2e;

   file_name :=
         'SALES_'
      || TO_CHAR (business_unit_id_in, 'FM00')
      || '_'
      || TO_CHAR (trans_ts, 'YYYYMMDD')
      || '_'
      || TO_CHAR (trans_ts, 'HH24MI')
      || '.dat';
   ft := UTL_FILE.fopen (loc_dir_in,
                         file_name,
                         'w',
                         32767
                        );
   ft_bak :=
      UTL_FILE.fopen (loc_bak_dir_in,
                      REPLACE (file_name,
                               '.dat',
                               '.bak'
                              ),
                      'w',
                      32767
                     );

-----------------------------------------------------
   FOR recc IN cur_iro
   LOOP
      IF recc.record_type = 'H'
      THEN
         UTL_FILE.put_line (ft,
                               recc.record_type
                            || ';'
                            || TO_CHAR (recc.business_unit_id)
                            || ';'
                            || ';'
                            || ';'
                            || recc.vendor_id
                            || ';'
                            || TO_CHAR (recc.sales_report_date, 'yyyymmdd')
                            || ';'
                            || TO_CHAR (recc.start_date, 'yyyymmdd')
                            || ';'
                            || TO_CHAR (recc.end_date, 'yyyymmdd')
                            || ';');
         UTL_FILE.put_line (ft_bak,
                               recc.record_type
                            || ';'
                            || TO_CHAR (recc.business_unit_id)
                            || ';'
                            || ';'
                            || ';'
                            || recc.vendor_id
                            || ';'
                            || TO_CHAR (recc.sales_report_date, 'yyyymmdd')
                            || ';'
                            || TO_CHAR (recc.start_date, 'yyyymmdd')
                            || ';'
                            || TO_CHAR (recc.end_date, 'yyyymmdd')
                            || ';');
      ELSIF recc.record_type = 'D'
      THEN
         UTL_FILE.put_line (ft,
                               recc.record_type
                            || ';'
                            || recc.style_id
                            || ';'
                            || recc.style_description
                            || ';'
                            || TO_CHAR (recc.section_id)
                            || ';');
         UTL_FILE.put_line (ft_bak,
                               recc.record_type
                            || ';'
                            || recc.style_id
                            || ';'
                            || recc.style_description
                            || ';'
                            || TO_CHAR (recc.section_id)
                            || ';');
      ELSIF recc.record_type = 'S'
      THEN
         IF recc.sales_amt <> 0
         THEN
            UTL_FILE.put_line (ft,
                                  recc.record_type
                               || ';'
                               || recc.site_id
                               || ';'                ----||recc.site_name||';'
                               || TO_CHAR (recc.sales_qty)
                               || ';'
                               || TO_CHAR (recc.sales_amt)
                               || ';'
                               || TO_CHAR (ROUND (  100
                                                  * recc.gp_amt
                                                  / recc.sales_amt,
                                                  2))
                               || ';');
            UTL_FILE.put_line (ft_bak,
                                  recc.record_type
                               || ';'
                               || recc.site_id
                               || ';'                ----||recc.site_name||';'
                               || TO_CHAR (recc.sales_qty)
                               || ';'
                               || TO_CHAR (recc.sales_amt)
                               || ';'
                               || TO_CHAR (ROUND (  100
                                                  * recc.gp_amt
                                                  / recc.sales_amt,
                                                  2))
                               || ';');
         ELSE
            UTL_FILE.put_line (ft,
                                  recc.record_type
                               || ';'
                               || recc.site_id
                               || ';'                ----||recc.site_name||';'
                               || TO_CHAR (recc.sales_qty)
                               || ';'
                               || TO_CHAR (recc.sales_amt)
                               || ';'
                               || NULL
                               || ';');
            UTL_FILE.put_line (ft_bak,
                                  recc.record_type
                               || ';'
                               || recc.site_id
                               || ';'                ----||recc.site_name||';'
                               || TO_CHAR (recc.sales_qty)
                               || ';'
                               || TO_CHAR (recc.sales_amt)
                               || ';'
                               || NULL
                               || ';');
         END IF;
      END IF;
   END LOOP;

   IF UTL_FILE.is_open (ft)
   THEN
      UTL_FILE.fclose (ft);
   END IF;

   IF UTL_FILE.is_open (ft_bak)
   THEN
      UTL_FILE.fclose (ft_bak);
   END IF;

   ft_ok :=
      UTL_FILE.fopen (loc_dir_in,
                      REPLACE (file_name,
                               '.dat',
                               '.ok'
                              ),
                      'w',
                      32767
                     );
   /* UTL_FILE.Put_Line(ft_ok,file_name||lpad(to_char(NVL(file_line,0)),10,'0')||
    to_char(trans_ts,'yyyymmddhh24miss'));*/
   UTL_FILE.put_line (ft_ok,
                         TO_CHAR (business_unit_id_in, 'FM00')
                      || ';'
                      || file_name
                      || ';'
                      || TO_CHAR (trans_ts, 'YYYYMMDD')
                      || ';'
                      || TO_CHAR (trans_ts, 'HH24MISS')
                      || ';'
                      || TO_CHAR (NVL (file_line, 0))
                      || ';');

   IF UTL_FILE.is_open (ft_ok)
   THEN
      UTL_FILE.fclose (ft_ok);
   END IF;

   INSERT INTO interface_batch_control_out
               (seq,
                trans_code,
                descirption,
                transaction_ts,
                business_unit_id,
                site_id,
                file_name,
                row_count,
                feeback_ts,
                feeback_row_count,
                ERROR_CODE,
                error_msg,
                status,
                local_dir
               )
   VALUES      ( /*cseq*/ 'SE' || TO_CHAR (trans_ts, 'YYYYMMDDHH24MISS'),
                RPAD (trans_code_in,
                      6,
                      '0'
                     ),
                RPAD (trans_code_in,
                      6,
                      '0'
                     ),
                SYSDATE,
                business_unit_id_in,
                'ldx',
                file_name,
                file_line,
                NULL,
                NULL,
                0,
                NULL,
                'GENERATED',
                loc_dir_in
               );

   COMMIT;
   RETURN 'Y';
EXCEPTION
   WHEN OTHERS
   THEN
      t_errcode := SQLCODE;
      t_errmsg := SUBSTR (SQLERRM,
                          1,
                          100
                         );
      ROLLBACK;

      INSERT INTO INTERFACE.interface_error_log
                  (seq,
                   trans_code,
                   descirption,
                   transaction_ts,
                   business_unit_id,
                   site_id,
                   file_name,
                   row_count,
                   feeback_ts,
                   feeback_row_count,
                   ERROR_CODE,
                   error_msg,
                   inbound_outbound,
                   status,
                   module
                  )
      VALUES      ( /*cseq*/ 'SE' || TO_CHAR (trans_ts, 'YYYYMMDDHH24MISS'),
                   RPAD (trans_code_in,
                         6,
                         '0'
                        ),
                   RPAD (trans_code_in,
                         6,
                         '0'
                        ),
                   SYSDATE,
                   business_unit_id_in,
                   site_id_in,
                   file_name,
                   0,
                   NULL,
                   NULL,
                   t_errcode,
                   t_errmsg,
                   'O',
                   'FAILED',
                   'lotus_intf_out_SALE2E'
                  );

      RETURN 'N';
END;
/