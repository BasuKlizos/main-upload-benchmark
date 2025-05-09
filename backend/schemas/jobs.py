from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, conint


class JobBase(BaseModel):
    title: str
    description: str
    tags: List[str]


class JobCreate(JobBase):
    pass


class Job(JobBase):
    id: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = False

    class Config:
        from_attributes = True


class JobDescription(BaseModel):
    location: str
    address: str
    job_post_name: str
    role_description: str
    experience: str
    responsibilities: List[str]
    requirements: List[str]
    must_haves: List[str]
    nice_to_have: List[str]
    preferred_skills: List[str]
    job_mode: str
    employment_type: str


class GenerateQuestions(BaseModel):
    job_id: str
    count: Optional[int] = conint(ge=1, le=50)
    save: bool = False


class UpdateQuestion(BaseModel):
    follow_up_count: Optional[int] = None
    delete: bool = False


class JobSelectionCriteria(BaseModel):
    resume_shortlist_criteria: int = conint(ge=0, le=100)
    screening_qualification_criteria: int = conint(ge=0, le=100)
    final_interview_criteria: int = conint(ge=0, le=100)


class JobSelectionProgress(BaseModel):
    total_candidate_count: int = conint(ge=0)
    shortlisted_candidate_count: int = conint(ge=0)
    screening_qualified_count: int = conint(ge=0)
    final_interview_qualified_count: int = conint(ge=0)
