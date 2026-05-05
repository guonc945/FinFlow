import type { ReactNode } from 'react';

type DataCenterSectionHeroProps = {
    eyebrow: string;
    title: string;
    description: string;
    aside?: ReactNode;
};

export default function DataCenterSectionHero({
    eyebrow,
    title,
    description,
    aside,
}: DataCenterSectionHeroProps) {
    return (
        <section className="card glass reporting-hero">
            <div>
                <div className="reporting-eyebrow">{eyebrow}</div>
                <h2>{title}</h2>
                <p className="reporting-copy">{description}</p>
            </div>
            {aside ? <div className="reporting-overview-grid">{aside}</div> : null}
        </section>
    );
}
