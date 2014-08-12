DROP VIEW LOTUS.V_LT_RTV_VENDOR_INFO;

CREATE OR REPLACE FORCE VIEW lotus.v_lt_rtv_vendor_info (business_unit_id,
                                                         claim_id,
                                                         claim_date,
                                                         grn,
                                                         region_id,
                                                         site_id,
                                                         vendor_id,
                                                         vendor_name,
                                                         address_id,
                                                         rtv_phone_vendor,
                                                         rtv_phone_address,
                                                         rtv_phone_contact,
                                                         rtv_phone,
                                                         rtv_fax_vendor,
                                                         rtv_fax_address,
                                                         rtv_fax_contact,
                                                         rtv_fax,
                                                         rtv_contact_name_vendor,
                                                         rtv_contact_name_contact,
                                                         rtv_contact_name,
                                                         rtv_address_1_vendor,
                                                         rtv_address_1_address,
                                                         rtv_address_1,
                                                         rtv_address_2_vendor,
                                                         rtv_address_2_address,
                                                         rtv_address_2
                                                        )
AS
   SELECT b.business_unit_id,
          b.claim_id,
          b.claim_date,
          b.grn,
          b.region_id,
          b.site_id,
          b.vendor_id,
          b.NAME,
          a.address_id,
          b.telephone rtv_phone_vendor,
          a.phone rtv_phone_address,
          t.phone rtv_phone_contact,
          NVL (NVL (a.phone, t.phone), b.telephone) rtv_phone,
          b.fax rtv_fax_vendor,
          a.fax rtv_fax_address,
          t.fax rtv_fax_contact,
          NVL (NVL (a.fax, t.fax), b.fax) rtv_fax,
          b.contact_first_name rtv_contact_name_vendor,
          t.contact_name rtv_contact_name_contact,
          NVL (t.contact_name, b.contact_first_name) rtv_contact_name,
          b.address_1 rtv_address_1_vendor,
          a.address_1 rtv_address_1_address,
          NVL (a.address_1, b.address_1) rtv_address_1,
          b.address_2 rtv_address_2_vendor,
          a.address_2 rtv_address_2_address,
          NVL (a.address_2, b.address_2) rtv_address_2
   FROM   (SELECT c.business_unit_id,
                  c.claim_id,
                  c.claim_date,
                  c.grn,
                  x.region_id,
                  c.site_id,
                  v.vendor_id,
                  v.NAME,
                  v.telephone,
                  v.fax,
                  v.contact_first_name,
                  v.address_1,
                  v.address_2
           FROM   vendors v,
                  claims c,
                  v_site_hierarchy x
           WHERE  c.business_unit_id = v.business_unit_id
           AND    c.vendor_id = v.vendor_id
           AND    c.business_unit_id = x.business_unit_id
           AND    c.site_id = x.site_id) b,
          address a,
          contact t
   WHERE  a.business_unit_id(+) = b.business_unit_id
   AND    SUBSTR (a.address_type_id(+),
                  3,
                  2
                 ) = b.region_id
   AND    a.address_type_id(+) LIKE 'RV%'
   AND    a.address_source_type_id(+) = 'VENDOR'
   AND    a.address_source_id(+) = b.vendor_id
   AND    t.business_unit_id(+) = a.business_unit_id
   AND    t.address_type_id(+) = a.address_type_id
   AND    t.address_id(+) = a.address_id;


DROP PUBLIC SYNONYM V_LT_RTV_VENDOR_INFO;

CREATE PUBLIC SYNONYM V_LT_RTV_VENDOR_INFO FOR LOTUS.V_LT_RTV_VENDOR_INFO;


GRANT SELECT ON LOTUS.V_LT_RTV_VENDOR_INFO TO PUBLIC;
