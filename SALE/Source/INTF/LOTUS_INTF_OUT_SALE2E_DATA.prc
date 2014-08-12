CREATE OR REPLACE PROCEDURE INTERFACE.lotus_intf_out_sale2e_data (
   business_unit_id_in                 VARCHAR2 := '88',
   p_vendor_id                         VARCHAR2
)
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
   v_section_id                  styles.section_id%TYPE;

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
               business_unit_id
      FROM     lotus_intf_sales_e2e a
      WHERE    a.business_unit_id = business_unit_id_in
      AND      a.end_date = d_week_ending_date
      AND      po_method = 'W'
      ORDER BY line_no;
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

   IF n_count > 0
   THEN                                -- Run only when data is not generated.
      IF v_flag_server = 'DEV'
      THEN
         -- delete all data
         DELETE FROM lotus_intf_sales_e2e;

         COMMIT;
      ELSIF v_flag_server = 'PRDTEST'
      THEN
         -- only delete all tested data
         DELETE FROM lotus_intf_sales_e2e
         WHERE       business_unit_id = business_unit_id_in
         AND         end_date = d_week_ending_date
         AND         po_method = 'W';

         COMMIT;
      ELSE
         NULL;
      END IF;
   ELSE
      -- delete history data
      DELETE FROM lotus_intf_sales_e2e
      WHERE       business_unit_id = business_unit_id_in
      AND         end_date < d_week_ending_date
      AND         po_method = 'W';

      COMMIT;
   END IF;

   v_old_vendor_id := 'NA';
   v_old_style_id := 'NA';
   v_vendor_id := p_vendor_id;
   v_bu := business_unit_id_in;

   FOR rec_style IN cur_style
   LOOP
      v_style_id := rec_style.style_id;
      v_site_id := rec_style.site_id;

      SELECT NAME
      INTO   v_site_name
      FROM   sites
      WHERE  business_unit_id = business_unit_id_in
      AND    site_id = v_site_id;

      IF v_old_vendor_id <> v_vendor_id
      THEN
         file_line := file_line + 1;

         INSERT INTO lotus_intf_sales_e2e
                     (record_type,
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
                      po_method
                     )
         VALUES      ('H',
                      v_vendor_id,
                      SYSDATE,
                      d_week_ending_date - 6,
                      d_week_ending_date,
                      NULL,
                      NULL,
                      NULL,
                      NULL,
                      NULL,
                      NULL,
                      NULL,
                      file_line,
                      v_bu,
                      'W'
                     );
      END IF;

      IF v_old_style_id <> v_style_id
      THEN
         file_line := file_line + 1;

         SELECT MAX (styles.description),
                MAX (section_id)
         INTO   v_style_desc,
                v_section_id
         FROM   styles
         WHERE  business_unit_id = v_bu
         AND    style_id = v_style_id;

         INSERT INTO lotus_intf_sales_e2e
                     (record_type,
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
                      po_method,
                      section_id
                     )
         VALUES      ('D',
                      v_vendor_id,
                      SYSDATE,
                      d_week_ending_date - 6,
                      d_week_ending_date,
                      v_style_id,
                      v_style_desc,
                      NULL,
                      NULL,
                      NULL,
                      NULL,
                      NULL,
                      file_line,
                      v_bu,
                      'W',
                      v_section_id
                     );
      END IF;

      file_line := file_line + 1;

      INSERT INTO lotus_intf_sales_e2e
                  (record_type,
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
                   po_method,
                   section_id
                  )
      VALUES      ('S',
                   v_vendor_id,
                   SYSDATE,
                   d_week_ending_date - 6,
                   d_week_ending_date,
                   v_style_id,
                   v_style_desc,
                   rec_style.site_id,
                   v_site_name,
                   rec_style.sales_qty,
                   rec_style.sales_amt,
                   NVL (rec_style.sales_amt, 0) - NVL (rec_style.COST, 0),
                   file_line,
                   v_bu,
                   'W',
                   v_section_id
                  );

      v_old_vendor_id := p_vendor_id;
      v_old_style_id := rec_style.style_id;
      COMMIT;
   END LOOP;

   SELECT COUNT (*)
   INTO   v_count
   FROM   lotus_intf_sales_e2e a
   WHERE  a.business_unit_id = business_unit_id_in
   AND    a.end_date = d_week_ending_date
   AND    a.po_method = 'W'
   AND    ROWNUM = 1;
EXCEPTION
   WHEN OTHERS
   THEN
      t_errcode := SQLCODE;
      t_errmsg := SUBSTR (SQLERRM,
                          1,
                          100
                         );
      ROLLBACK;
END;
/