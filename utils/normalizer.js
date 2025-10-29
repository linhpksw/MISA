function normalizeHeader(header) {
    if (header === undefined || header === null) return null;
    return String(header)
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/đ/g, 'd')
        .replace(/Đ/g, 'D')
        .trim()
        .replace(/\s+/g, ' ')
        .replace(/[^\w ]/g, '')
        .replace(/\s+/g, '_')
        .toLowerCase();
}

function toCamelCase(str) {
    return String(str || '')
        .replace(/[_\s]+(.)?/g, (_, chr) => (chr ? chr.toUpperCase() : ''))
        .replace(/^[A-Z]/, (c) => c.toLowerCase());
}

function mapOdooRelation(rel) {
    if (!rel) return null;
    if (Array.isArray(rel)) {
        const [id, name] = rel;
        if (id === undefined && name === undefined) return null;
        return { id: id ?? null, name: name ?? null };
    }
    if (typeof rel === 'object') {
        return {
            id: rel.id ?? null,
            name: rel.display_name ?? rel.name ?? null,
        };
    }
    return { id: null, name: String(rel) };
}

module.exports = {
    normalizeHeader,
    toCamelCase,
    mapOdooRelation,
};
