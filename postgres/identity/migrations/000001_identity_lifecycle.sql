-- +migrations Up
CREATE TABLE public.identity_outbox_bindings (
 effect_id text PRIMARY KEY CHECK (effect_id ~ '^[0-9a-f]{64}$'),
 tenant_id text NOT NULL CHECK (octet_length(tenant_id) BETWEEN 1 AND 255),
 command_id bytea NOT NULL CHECK (octet_length(command_id)=16),
 event_index integer NOT NULL CHECK (event_index BETWEEN 0 AND 65535),
 canonical_sha256 bytea NOT NULL CHECK (octet_length(canonical_sha256)=32),
 UNIQUE(tenant_id,command_id,event_index)
);

CREATE FUNCTION public.identity_outbox_reject_binding_mutation()
RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='identity outbox binding is immutable';
END;
$$;
REVOKE ALL ON FUNCTION public.identity_outbox_reject_binding_mutation() FROM PUBLIC;
CREATE TRIGGER identity_outbox_binding_immutable
 BEFORE UPDATE OR DELETE ON public.identity_outbox_bindings
 FOR EACH ROW EXECUTE FUNCTION public.identity_outbox_reject_binding_mutation();
CREATE TRIGGER identity_outbox_binding_no_truncate
 BEFORE TRUNCATE ON public.identity_outbox_bindings
 FOR EACH STATEMENT EXECUTE FUNCTION public.identity_outbox_reject_binding_mutation();

COMMENT ON TABLE public.identity_outbox_bindings IS
 'Retained immutable identity command/index bindings; payload pruning never releases effect IDs';

-- +migrations Down
DO $$ BEGIN
 RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='identity outbox binding downgrade unsupported';
END $$;
