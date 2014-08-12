CREATE OR REPLACE FUNCTION INTERFACE.lotus_intf_out_rtve2e (
   trans_code_in                       VARCHAR2 := 'RTVE2E',
   business_unit_id_in                 VARCHAR2 := '88',
   site_id_in                          VARCHAR2 := '000',
   loc_dir_in                          VARCHAR2 := 'LOTUS_PE2E',
   loc_bak_dir_in                      VARCHAR2 := 'LOTUS_PE2E_HISTORY'
)
   RETURN VARCHAR2
IS
/*Created by Ben 2012.3.13*/
   n_count                       NUMBER (16);
   t_errcode                     NUMBER (5);
   t_errmsg                      VARCHAR2 (200);
   trans_ts                      VARCHAR2 (20) := NULL;
   ft                            UTL_FILE.file_type;
   ft_bak                        UTL_FILE.file_type;
   ft_ok                         UTL_FILE.file_type;
   cseq                          VARCHAR2 (12);
   file_name                     VARCHAR2 (500);
   first_loop                    BOOLEAN := TRUE;
   site_spec                     BOOLEAN := TRUE;
   site_file                     VARCHAR2 (500);
   site_seq                      VARCHAR2 (20);
   file_line                     NUMBER (16) := 0;

   CURSOR cur_iro
   IS
      SELECT   business_unit_id,
               claim_id
      FROM     iro_pos_return_to_vendor a
      WHERE    a.business_unit_id = business_unit_id_in
      AND      a.site_id = DECODE (site_id_in,
                                   '000', a.site_id,
                                   site_id_in
                                  )
      AND      a.lt_process_status = 'N'
      AND      a.lt_process_date = trans_ts
      AND      a.pos_verifies_rtv_status_ind = '1'
      --AND      claim_id IN (1029044303, 1029044304)
      GROUP BY business_unit_id,
               claim_id
      ORDER BY business_unit_id,
               claim_id;

   CURSOR cur_get_header (
      cp_bui                              NUMBER,
      cp_claim_id                         NUMBER
   )
   IS
      SELECT 'H' TYPE,
             v.vendor_id vendor_id,
             v.vendor_name vendor_name,
             v.rtv_address_1 || rtv_address_2 vendor_rtv_address,
             v.grn grn,
             s.site_id site_id,
             s.address_1 site_address1,
             s.address_1 site_address2,
             s.fax site_fax,
             s.telephone site_phone,
             v.claim_id claim_id,
             TO_CHAR (v.claim_date, 'yyyymmdd') claim_date,
             v.rtv_contact_name contact_name,
             v.rtv_fax contact_fax,
             v.rtv_phone contact_phone
      FROM   v_lt_rtv_vendor_info v,
             sites s
      WHERE  v.business_unit_id = s.business_unit_id
      AND    v.site_id = s.site_id
      AND    v.business_unit_id = cp_bui
      AND    v.claim_id = cp_claim_id;

   CURSOR cur_get_dtl (
      cp_bui                              NUMBER,
      cp_claim_id                         NUMBER
   )
   IS
      SELECT   'D' TYPE,
               c.style_id style_id,
               c.bar_code_id sku,
               s.description description,
               c.color_id color_id,
               b.description color_desc,
               c.size_id size_id,
               c.dimension_id dimension_id,
               c.item_qty item_qty,
               c.unit_cost unit_cost,
               c.claim_amount,
               c.claim_vat,
               r.description reason_desc
      FROM     claim_details c,
               styles s,
               colors b,
               reasons r
      WHERE    c.business_unit_id = s.business_unit_id
      AND      c.style_id = s.style_id
      AND      c.business_unit_id = b.business_unit_id
      AND      c.color_id = b.color_id
      AND      c.business_unit_id = r.business_unit_id
      AND      c.returned_reason_id = r.reason_id
      AND      c.business_unit_id = cp_bui
      AND      c.claim_id = cp_claim_id
      ORDER BY c.style_id;
BEGIN
   SELECT TO_CHAR (SYSDATE, 'yyyymmddhh24miss')
   INTO   trans_ts
   FROM   DUAL;

   UPDATE iro_pos_return_to_vendor a
   SET a.lt_process_date = trans_ts
   WHERE  a.business_unit_id = business_unit_id_in
   AND    a.site_id = DECODE (site_id_in,
                              '000', a.site_id,
                              site_id_in
                             )
   AND    a.lt_process_status = 'N'
   AND    a.lt_process_date IS NULL
   AND    a.pos_verifies_rtv_status_ind = '1';
   
   file_name :=
         'RTV_'
      || TO_CHAR (business_unit_id_in, 'FM00')
      || '_'
      || SUBSTR (trans_ts,
                 1,
                 8
                )
      || '_'
      || SUBSTR (trans_ts,
                 9,
                 6
                )
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

   FOR rec_iro IN cur_iro
   LOOP
      FOR rec_h IN cur_get_header (rec_iro.business_unit_id,
                                   rec_iro.claim_id)
      LOOP
         UTL_FILE.put_line (ft,
                               rec_h.TYPE
                            || ';'
                            || rec_h.vendor_id
                            || ';'
                            || rec_h.vendor_name
                            || ';'
                            || rec_h.vendor_rtv_address
                            || ';'
                            || rec_h.grn
                            || ';'
                            || rec_h.site_id
                            || ';'
                            || rec_h.site_address1
                            || ';'
                            || rec_h.site_address2
                            || ';'
                            || rec_h.site_fax
                            || ';'
                            || rec_h.site_phone
                            || ';'
                            || rec_h.claim_id
                            || ';'
                            || rec_h.claim_date
                            || ';'
                            || rec_h.contact_name
                            || ';'
                            || rec_h.contact_fax
                            || ';'
                            || rec_h.contact_phone
                            || ';');
         UTL_FILE.put_line (ft_bak,
                               rec_h.TYPE
                            || ';'
                            || rec_h.vendor_id
                            || ';'
                            || rec_h.vendor_name
                            || ';'
                            || rec_h.vendor_rtv_address
                            || ';'
                            || rec_h.grn
                            || ';'
                            || rec_h.site_id
                            || ';'
                            || rec_h.site_address1
                            || ';'
                            || rec_h.site_address2
                            || ';'
                            || rec_h.site_fax
                            || ';'
                            || rec_h.site_phone
                            || ';'
                            || rec_h.claim_id
                            || ';'
                            || rec_h.claim_date
                            || ';'
                            || rec_h.contact_name
                            || ';'
                            || rec_h.contact_fax
                            || ';'
                            || rec_h.contact_phone
                            || ';');
         file_line := file_line + 1;

         FOR rec_d IN cur_get_dtl (rec_iro.business_unit_id, rec_iro.claim_id)
         LOOP
            UTL_FILE.put_line (ft,
                                  rec_d.TYPE
                               || ';'
                               || rec_d.style_id
                               || ';'
                               || rec_d.sku
                               || ';'
                               || rec_d.description
                               || ';'
                               || rec_d.color_id
                               || ';'
                               || rec_d.color_desc
                               || ';'
                               || rec_d.size_id
                               || ';'
                               || rec_d.dimension_id
                               || ';'
                               || rec_d.item_qty
                               || ';'
                               || rec_d.unit_cost
                               || ';'
                               || rec_d.claim_amount
                               || ';'
                               || rec_d.claim_vat
                               || ';'
                               || rec_d.reason_desc
                               || ';');
            UTL_FILE.put_line (ft_bak,
                                  rec_d.TYPE
                               || ';'
                               || rec_d.style_id
                               || ';'
                               || rec_d.sku
                               || ';'
                               || rec_d.description
                               || ';'
                               || rec_d.color_id
                               || ';'
                               || rec_d.color_desc
                               || ';'
                               || rec_d.size_id
                               || ';'
                               || rec_d.dimension_id
                               || ';'
                               || rec_d.item_qty
                               || ';'
                               || rec_d.unit_cost
                               || ';'
                               || rec_d.claim_amount
                               || ';'
                               || rec_d.claim_vat
                               || ';'
                               || rec_d.reason_desc
                               || ';');
            file_line := file_line + 1;
         END LOOP;
      END LOOP;
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
                      || SUBSTR (trans_ts,
                                 1,
                                 8
                                )
                      || ';'
                      || SUBSTR (trans_ts,
                                 9,
                                 6
                                )
                      || ';'
                      || TO_CHAR (NVL (file_line, 0))
                      || ';');

   IF UTL_FILE.is_open (ft_ok)
   THEN
      UTL_FILE.fclose (ft_ok);
   END IF;

   UPDATE iro_pos_return_to_vendor a
   SET a.lt_process_status = 'Y'
   WHERE  a.business_unit_id = business_unit_id_in
   AND    a.site_id = DECODE (site_id_in,
                              '000', a.site_id,
                              site_id_in
                             )
   AND    a.lt_process_status = 'N'
   AND    a.lt_process_date = trans_ts
   AND    a.pos_verifies_rtv_status_ind = '1';

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
   VALUES      ( /*cseq*/ 'SE' || trans_ts,
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

      INSERT INTO interface_error_log
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
      VALUES      (cseq,
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
                   'lotus_intf_out_rtve2e'
                  );

      COMMIT;
      RETURN 'N';
END;
/


DROP PUBLIC SYNONYM LOTUS_INTF_OUT_RTVE2E;

CREATE PUBLIC SYNONYM LOTUS_INTF_OUT_RTVE2E FOR INTERFACE.LOTUS_INTF_OUT_RTVE2E;

GRANT EXECUTE ON INTERFACE.LOTUS_INTF_OUT_RTVE2E TO "PUBLIC";